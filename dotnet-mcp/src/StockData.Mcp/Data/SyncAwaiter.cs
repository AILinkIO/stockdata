using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// MCP 交互读的「水位检查 + Drainer 等待」包装（替代 ReadFetch 的有界抓取模型）。
///
/// pgOnly 模式下三分支决策：
/// 1. 水位 Fresh（无缺口）→ 立即返回
/// 2. 不 Fresh + 水位存在（有旧数据）→ 返回 stale，后台 Drainer 刷新
/// 3. 不 Fresh + 水位不存在（首次触达）→ 轮询水位等 Drainer 完成（默认 5 分钟超时）
///
/// 非 pgOnly 模式（旧读穿透）→ 直接调用 ensure，行为不变。
/// </summary>
internal static class SyncAwaiter
{
    public static Task EnsureAsync(
        IConfiguration config, bool pgOnly, ILogger? logger, TimeProvider time, CancellationToken ct,
        Func<CancellationToken, Task<(Decision Decision, bool HasExistingData)>> checkCoverage,
        Func<CancellationToken, Task> ensure)
        => pgOnly ? WaitAsync(config, logger, time, ct, checkCoverage) : ensure(ct);

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

    private static async Task WaitAsync(
        IConfiguration config, ILogger? logger, TimeProvider time, CancellationToken ct,
        Func<CancellationToken, Task<(Decision Decision, bool HasExistingData)>> checkCoverage)
    {
        var (decision, _) = await checkCoverage(ct);
        if (decision.Fresh) return;

        var budget = config.GetValue("StockData:ReadWaitDrainerSeconds", 300);
        var interval = config.GetValue("StockData:ReadPollIntervalSeconds", 3);
        var deadline = time.GetUtcNow().AddSeconds(budget);

        while (time.GetUtcNow() < deadline)
        {
            await Task.Delay(TimeSpan.FromSeconds(interval), time, ct);
            (decision, _) = await checkCoverage(ct);
            if (decision.Fresh) return;
        }

        logger?.LogWarning("SyncAwaiter 等待 Drainer 超时（{Budget}s），回退 PG 现状", budget);
    }
}
