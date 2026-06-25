namespace StockData.Mcp.Data;

/// <summary>
/// 水位输入（移植自 <c>db.models.DataWatermark</c> 的判定相关字段）。
/// 保持 Coverage 与 EF 实体解耦：覆盖度判定只依赖这三个值，纯函数易测。
/// </summary>
public sealed record Watermark(DateOnly? FirstDate, DateOnly LastDate, DateTimeOffset LastFetchedAt);

/// <summary>覆盖度判定结果。FetchRanges 为空即可直接读库（Fresh）。</summary>
public sealed record Decision(IReadOnlyList<(DateOnly Start, DateOnly End)> FetchRanges, string Reason)
{
    public bool Fresh => FetchRanges.Count == 0;

    /// <summary>构造"无需抓取"判定（FetchRanges 为空）。</summary>
    public static Decision Covered(string reason) => new(Array.Empty<(DateOnly, DateOnly)>(), reason);
}
