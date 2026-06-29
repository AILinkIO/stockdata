using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// MCP 交互读的「水位检查 + 定向高优先抓取」包装。
///
/// pgOnly 模式下两分支决策：
/// 1. 水位 Fresh（无缺口）→ 立即返回，调用方直读 PG
/// 2. 不 Fresh → 以 high 优先级主动发起 ensure 抓取缺口（有界预算 ReadFetchBudgetSeconds）。
///    抓取完成后调用方读 PG 全量数据（旧 + 新）；**超预算/失败则抛 FetchTimeoutException /
///    FetchFailedException**——由 <see cref="GuardAsync"/> 兜底转 "Error: ..." 字符串返回调用方，
///    不再静默回退 PG 现状。
///
/// 非 pgOnly 模式（旧读穿透）→ 直接调用 ensure，行为不变。
/// </summary>
internal static class SyncAwaiter
{
    public static Task EnsureAsync(
        IConfiguration config, bool pgOnly, ILogger? logger, TimeProvider time, CancellationToken ct,
        Func<CancellationToken, Task<(Decision Decision, bool HasExistingData)>> checkCoverage,
        Func<CancellationToken, Task> ensure)
        => pgOnly ? FetchAsync(config, logger, ct, checkCoverage, ensure) : ensure(ct);

    /// <summary>CheckRange 数据类型的 checkCoverage 构造器（K线/日历/宏观/分红/express/forecast）。</summary>
    public static Func<CancellationToken, Task<(Decision, bool)>> RangeCheck(
        IWatermarkStore watermarks, string code, string dataType,
        DateOnly start, DateOnly end, DateTimeOffset now)
        => async ct =>
        {
            var wm = await watermarks.GetAsync(code, dataType, ct);
            return (Coverage.CheckRange(wm?.ToWatermark(), dataType, start, end, now), wm is not null);
        };

    /// <summary>CheckQuarter 数据类型的 checkCoverage 构造器（六类季度财报）。</summary>
    public static Func<CancellationToken, Task<(Decision, bool)>> QuarterCheck(
        IWatermarkStore watermarks, StockDataDbContext db, string code,
        int year, int quarter, DateTimeOffset now)
        => async ct =>
        {
            var statDate = Coverage.QuarterEnd(year, quarter);
            var hasRows = await db.FinancialReports.AsNoTracking()
                .AnyAsync(r => r.Code == code && r.StatDate == statDate, ct);
            var wm = await watermarks.GetAsync(code, $"fin:{year}q{quarter}", ct);
            return (Coverage.CheckQuarter(hasRows, wm?.LastFetchedAt, year, quarter, now),
                hasRows || wm is not null);
        };

    /// <summary>CheckSnapshot 数据类型的 checkCoverage 构造器（stock_basic/stock_list/index/industry）。</summary>
    public static Func<CancellationToken, Task<(Decision, bool)>> SnapshotCheck(
        IWatermarkStore watermarks, string code, string dataType,
        DateOnly snapDate, DateTimeOffset now)
        => async ct =>
        {
            var wm = await watermarks.GetAsync(code, dataType, ct);
            var hasRows = wm is not null;
            return (Coverage.CheckSnapshot(wm?.ToWatermark(), dataType, snapDate, hasRows, now), hasRows);
        };

    /// <summary>CheckAdjustFactor 数据类型的 checkCoverage 构造器（复权因子）。</summary>
    public static Func<CancellationToken, Task<(Decision, bool)>> AdjustFactorCheck(
        IWatermarkStore watermarks, StockDataDbContext db, string code, DateTimeOffset now)
        => async ct =>
        {
            var afWm = await watermarks.GetAsync(code, "adjust_factor", ct);
            var afMaxEvent = await db.AdjustFactors.AsNoTracking()
                .Where(a => a.Code == code).MaxAsync(a => (DateOnly?)a.DividOperateDate, ct);
            var divMaxEvent = await db.Dividends.AsNoTracking()
                .Where(d => d.Code == code && d.OperateDate != null)
                .MaxAsync(d => (DateOnly?)d.OperateDate, ct);
            return (Coverage.CheckAdjustFactor(afWm?.ToWatermark(), afMaxEvent, divMaxEvent, now),
                afWm is not null);
        };

    /// <summary>
    /// 定向高优先有界抓取：Fresh 时跳过；否则以 high 优先级调 ensure 补缺口。
    /// 预算内完成 → 调用方读到完整数据；**超预算 → 转 FetchTimeoutException 抛出；fetch 失败 →
    /// FetchFailedException 直接传播**——不再吞异常静默回退 PG，由 <see cref="GuardAsync"/> 兜底。
    /// </summary>
    private static async Task FetchAsync(
        IConfiguration config, ILogger? logger, CancellationToken ct,
        Func<CancellationToken, Task<(Decision Decision, bool HasExistingData)>> checkCoverage,
        Func<CancellationToken, Task> ensure)
    {
        var (decision, _) = await checkCoverage(ct);
        if (decision.Fresh) return;

        var budget = config.GetValue("StockData:ReadFetchBudgetSeconds", 30);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (budget > 0) cts.CancelAfter(TimeSpan.FromSeconds(budget));
        using (FetchPriority.High())
        {
            try
            {
                await ensure(cts.Token);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                // 读路径 budget 超时：不静默回退，转为 FetchTimeoutException 让调用方知道数据未同步完成
                logger?.LogWarning("SyncAwaiter budget 超时（{Budget}s），抛出 FetchTimeoutException", budget);
                throw new FetchTimeoutException($"读路径抓取预算超时（{budget}s），数据未同步完成");
            }
            // FetchTimeoutException / FetchFailedException 直接向上传播，不再吞掉
        }
    }

    /// <summary>
    /// 读路径统一兜底：抓取失败（<see cref="FetchTimeoutException"/> / <see cref="FetchFailedException"/>）
    /// → 返回 <c>"Error: 数据同步未完成：..."</c> 字符串。**不静默回退 PG**——调用方（MCP 工具 /
    /// KlineLoader）据 <c>"Error:"</c> 前缀判失败并透传给客户端。
    ///
    /// 各 ReadService 的公开方法用 <c>=> SyncAwaiter.GuardAsync(async () => { ... })</c> 包裹整个方法体。
    /// </summary>
    public static async Task<string> GuardAsync(Func<Task<string>> action)
    {
        try { return await action(); }
        catch (Exception ex) when (ex is FetchTimeoutException or FetchFailedException)
        {
            return $"Error: 数据同步未完成：{ex.Message}";
        }
    }
}
