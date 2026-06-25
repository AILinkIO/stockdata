using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>水位读取（编排只需读，写在 writer 的同事务里）。</summary>
public interface IWatermarkStore
{
    Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default);
}

/// <summary>
/// 落盘：把一段 payload 的行 upsert + 推进水位，**同一事务**（复刻 SIGKILL 全提交/全回滚）。
/// 返回写入行数。last_date 由 ClaimableLast 算（未定型尾部只认实际数据，防永久空洞）。
/// </summary>
public interface IKlineWriter
{
    Task<int> PersistAsync(
        string code, string frequency, string dataType, FetchPayload payload,
        DateOnly sliceStart, DateOnly sliceEnd, DateTimeOffset now, CancellationToken ct = default);
}

/// <summary>
/// 日线读穿透编排（P4 端到端竖切）：coverage 判定 → 切片 → 逐段抓取+落盘。
/// 返回后即保证请求范围已覆盖且新鲜，调用方（MCP 工具）直读 PG。
/// 三个依赖均注入，便于用 fake 单测整条链路而不触 Python/baostock。
/// </summary>
public sealed class KlineService(IWatermarkStore watermarks, IFetchClient fetch, IKlineWriter writer)
{
    public async Task EnsureRangeAsync(
        string code, string dataType, DateOnly start, DateOnly end, DateTimeOffset now, CancellationToken ct = default)
    {
        var wmEntity = await watermarks.GetAsync(code, dataType, ct);
        var decision = Coverage.CheckRange(wmEntity?.ToWatermark(), dataType, start, end, now);
        if (decision.Fresh) return;

        var frequency = dataType[2..];                 // "k_d" → "d"
        var maxDays = RangeSlicer.SliceDays(dataType);

        foreach (var (fs, fe) in decision.FetchRanges)
        foreach (var (ss, se) in RangeSlicer.Slice(fs, fe, maxDays))
        {
            var payload = await fetch.FetchAsync(new FetchRequest(code, ss, se, frequency), ct);
            await writer.PersistAsync(code, frequency, dataType, payload, ss, se, now, ct);
        }
    }
}
