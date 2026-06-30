using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data;

/// <summary>
/// 交易日派生工具（移植 api/services/dates.py）：基于已迁的 trade_calendar 计算。
/// 核心 _trading_days(start,end) = EnsureRange(日历) + 查区间内交易日。45 天回看覆盖最长假期。
/// 始终注册，Enabled 反映开关。返回 JSON 形状对齐旧 /api/v1/dates/*。
/// </summary>
public sealed class TradingDaysReadService(IServiceProvider root, IConfiguration config)
{
    private const int Lookback = 45;

    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    private async Task<List<DateOnly>> TradingDaysAsync(DateOnly start, DateOnly end, CancellationToken ct)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var now = tp.GetUtcNow();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, "", "trade_calendar", start, end, now),
            c => sp.GetRequiredService<TradeCalendarService>().EnsureRangeAsync(start, end, now, c));
        return await db.TradeCalendars.AsNoTracking()
            .Where(c => c.CalendarDate >= start && c.CalendarDate <= end && c.IsTradingDay)
            .OrderBy(c => c.CalendarDate).Select(c => c.CalendarDate).ToListAsync(ct);
    }

    private static DateOnly Today() => Coverage.Today(TimeProvider.System.GetUtcNow());

    public Task<string> LatestTradingDateAsync(CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        var t = Today();
        var days = await TradingDaysAsync(t.AddDays(-Lookback), t, ct);
        return days.Count == 0 ? "Error: 交易日历数据缺失" : Obj(("date", Iso(days[^1])));
    });

    public Task<string> IsTradingDayAsync(DateOnly d, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        var days = await TradingDaysAsync(d, d, ct);
        return $$"""{"date":"{{Iso(d)}}","is_trading_day":{{(days.Count > 0 ? "true" : "false")}}}""";
    });

    public Task<string> PreviousTradingDayAsync(DateOnly d, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        var days = await TradingDaysAsync(d.AddDays(-Lookback), d.AddDays(-1), ct);
        return days.Count == 0 ? $"Error: {Iso(d)} 之前 {Lookback} 天内无交易日" : Obj(("date", Iso(days[^1])));
    });

    public Task<string> NextTradingDayAsync(DateOnly d, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        var days = await TradingDaysAsync(d.AddDays(1), d.AddDays(Lookback), ct);
        return days.Count == 0 ? $"Error: {Iso(d)} 之后 {Lookback} 天内无交易日" : Obj(("date", Iso(days[0])));
    });

    public Task<string> LastNTradingDaysAsync(int n, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        if (n <= 0) return "Error: days 必须为正数";
        var t = Today();
        var days = await TradingDaysAsync(t.AddDays(-(n * 3 + Lookback)), t, ct);
        var last = days.Count > n ? days[^n..] : days;
        return $$"""{"dates":[{{string.Join(",", last.Select(d => $"\"{Iso(d)}\""))}}]}""";
    });

    public Task<string> RecentRangeAsync(int n, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        if (n <= 0) return "Error: days 必须为正数";
        var t = Today();
        var days = await TradingDaysAsync(t.AddDays(-(n * 3 + Lookback)), t, ct);
        if (days.Count == 0) return "Error: 交易日历数据缺失";
        var last = days.Count > n ? days[^n..] : days;
        return $$"""{"start_date":"{{Iso(last[0])}}","end_date":"{{Iso(last[^1])}}"}""";
    });

    private static string Iso(DateOnly d) => d.ToString("yyyy-MM-dd");
    private static string Obj(params (string K, string V)[] kv)
        => "{" + string.Join(",", kv.Select(p => $"\"{p.K}\":\"{p.V}\"")) + "}";
}
