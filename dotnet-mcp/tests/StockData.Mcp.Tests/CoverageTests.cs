using StockData.Mcp.Data;

namespace StockData.Mcp.Tests;

/// <summary>
/// Coverage 移植的黄金对照测试——逐 case 对齐 Python <c>server/tests/test_coverage.py</c>。
/// 方法名沿用 Python 测试函数名，便于 1:1 追溯（迁移 TASK §5 / k_d 方案 §5 清单）。
/// 固定"现在"：2026-06-11（周四）12:00 UTC+8。
/// </summary>
public class CoverageTests
{
    private static readonly DateTimeOffset NOW = new(2026, 6, 11, 12, 0, 0, TimeSpan.FromHours(8));
    private static readonly DateOnly TODAY = new(2026, 6, 11);

    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    private static Watermark Wm(DateOnly first, DateOnly last, long fetchedAgoSeconds = 0)
        => new(first, last, NOW.AddSeconds(-fetchedAgoSeconds));

    private static (DateOnly, DateOnly)[] Ranges(params (DateOnly, DateOnly)[] r) => r;

    // ── check_range：通用规则 ──

    [Fact]
    public void test_no_watermark_full_backfill()
    {
        var d = Coverage.CheckRange(null, "k_d", D(2024, 1, 1), D(2024, 12, 31), NOW);
        Assert.False(d.Fresh);
        Assert.Equal(Ranges((D(1990, 12, 19), D(2024, 12, 31))), d.FetchRanges);
    }

    [Fact]
    public void test_minute_backfill_start_respected()
    {
        var d = Coverage.CheckRange(null, "k_30", D(2024, 1, 1), D(2024, 6, 1), NOW);
        Assert.True(d.FetchRanges[0].Start >= D(2023, 1, 1));
    }

    [Fact]
    public void test_covered_history_is_fresh_forever()
    {
        var w = Wm(D(2020, 1, 1), D(2024, 12, 31), 10_000_000);
        Assert.True(Coverage.CheckRange(w, "k_d", D(2024, 1, 1), D(2024, 12, 31), NOW).Fresh);
    }

    [Fact]
    public void test_tail_gap()
    {
        var w = Wm(D(2020, 1, 1), D(2025, 12, 31));
        var d = Coverage.CheckRange(w, "k_d", D(2025, 1, 1), D(2026, 6, 10), NOW);
        Assert.Equal(Ranges((D(2026, 1, 1), D(2026, 6, 10))), d.FetchRanges);
    }

    [Fact]
    public void test_head_gap()
    {
        var w = Wm(D(2020, 1, 1), D(2025, 12, 31));
        var d = Coverage.CheckRange(w, "k_d", D(2019, 1, 1), D(2020, 6, 1), NOW);
        Assert.Equal(Ranges((D(2019, 1, 1), D(2019, 12, 31))), d.FetchRanges);
    }

    [Fact]
    public void test_today_fresh_within_interval()
    {
        var w = Wm(D(2020, 1, 1), TODAY, 60);
        Assert.True(Coverage.CheckRange(w, "k_d", D(2026, 6, 1), TODAY, NOW).Fresh);
    }

    [Fact]
    public void test_today_stale_after_interval()
    {
        var w = Wm(D(2020, 1, 1), TODAY, 600);
        var d = Coverage.CheckRange(w, "k_d", D(2026, 6, 1), TODAY, NOW);
        Assert.Equal(Ranges((TODAY, TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_tail_gap_and_stale_merge()
    {
        var w = Wm(D(2020, 1, 1), TODAY.AddDays(-3), 600);
        var d = Coverage.CheckRange(w, "k_d", D(2026, 6, 1), TODAY, NOW);
        Assert.Equal(Ranges((TODAY.AddDays(-2), TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_unsettled_tail_gap_throttled()
    {
        var w = Wm(D(2020, 1, 1), TODAY.AddDays(-1), 60);
        Assert.True(Coverage.CheckRange(w, "k_d", D(2026, 6, 1), TODAY, NOW).Fresh);
        var wStale = Wm(D(2020, 1, 1), TODAY.AddDays(-1), 600);
        var d = Coverage.CheckRange(wStale, "k_d", D(2026, 6, 1), TODAY, NOW);
        Assert.Equal(Ranges((TODAY, TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_settled_tail_gap_not_throttled()
    {
        var w = Wm(D(2020, 1, 1), TODAY.AddDays(-3), 60);
        var d = Coverage.CheckRange(w, "k_d", D(2026, 6, 1), TODAY, NOW);
        Assert.Equal(Ranges((TODAY.AddDays(-2), TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_claim_made_while_unsettled_is_reverified()
    {
        // 永久空洞回归（2026-06-11 中国联通事故）：昨日盘后抓过昨日（水位声明到 6/10、
        // 抓取时刻昨日 17:00），该日如今已定型——仍须以"上次抓取时定型边界"为起点重核实。
        var yesterday = TODAY.AddDays(-1);
        var w = new Watermark(D(2020, 1, 1), yesterday, new DateTimeOffset(2026, 6, 10, 17, 0, 0, TimeSpan.FromHours(8)));
        var d = Coverage.CheckRange(w, "k_d", yesterday, yesterday, NOW);
        Assert.Equal(Ranges((yesterday, yesterday)), d.FetchRanges);
    }

    // ── 周/月线定型边界 ──

    [Fact]
    public void test_weekly_settled_boundary_is_monday()
        => Assert.Equal(D(2026, 6, 7), Coverage.SettledBoundary("k_w", TODAY));

    [Fact]
    public void test_weekly_past_week_fresh_even_if_old_fetch()
    {
        var w = Wm(D(2020, 1, 1), D(2026, 6, 5), 3 * 86400L);
        Assert.True(Coverage.CheckRange(w, "k_w", D(2026, 5, 1), D(2026, 6, 5), NOW).Fresh);
    }

    [Fact]
    public void test_weekly_bar_fetched_midweek_reverified()
    {
        // 上周四（6/4）抓到的 6/5 周线 bar 当时尚未定型（周线按周定型），重新核实
        var w = new Watermark(D(2020, 1, 1), D(2026, 6, 5), new DateTimeOffset(2026, 6, 4, 15, 0, 0, TimeSpan.FromHours(8)));
        var d = Coverage.CheckRange(w, "k_w", D(2026, 5, 1), D(2026, 6, 5), NOW);
        Assert.Equal(Ranges((D(2026, 6, 1), D(2026, 6, 5))), d.FetchRanges);
    }

    [Fact]
    public void test_weekly_current_week_stale()
    {
        var w = Wm(D(2020, 1, 1), TODAY, 600);
        var d = Coverage.CheckRange(w, "k_w", D(2026, 5, 1), TODAY, NOW);
        Assert.Equal(Ranges((D(2026, 6, 8), TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_monthly_settled_boundary()
        => Assert.Equal(D(2026, 5, 31), Coverage.SettledBoundary("k_m", TODAY));

    // ── 季度财报 ──

    [Fact]
    public void test_quarter_deadline_table()
    {
        Assert.Equal(D(2025, 4, 30), Coverage.QuarterDisclosureDeadline(2025, 1));
        Assert.Equal(D(2025, 8, 31), Coverage.QuarterDisclosureDeadline(2025, 2));
        Assert.Equal(D(2025, 10, 31), Coverage.QuarterDisclosureDeadline(2025, 3));
        Assert.Equal(D(2026, 4, 30), Coverage.QuarterDisclosureDeadline(2025, 4));
    }

    [Fact]
    public void test_quarter_never_fetched()
    {
        var d = Coverage.CheckQuarter(false, null, 2024, 3, NOW);
        Assert.Equal(Ranges((D(2024, 7, 1), D(2024, 9, 30))), d.FetchRanges);
    }

    [Fact]
    public void test_quarter_settled_permanent()
        => Assert.True(Coverage.CheckQuarter(true, NOW.AddDays(-300), 2024, 3, NOW).Fresh);

    [Fact]
    public void test_quarter_in_disclosure_window_stale()
    {
        var d = Coverage.CheckQuarter(true, NOW.AddDays(-2), 2026, 2, NOW);
        Assert.Equal(Ranges((D(2026, 4, 1), D(2026, 6, 30))), d.FetchRanges);
    }

    [Fact]
    public void test_quarter_in_disclosure_window_fresh()
        => Assert.True(Coverage.CheckQuarter(true, NOW.AddHours(-1), 2026, 2, NOW).Fresh);

    [Fact]
    public void test_quarter_hole_not_masked_by_other_quarters()
        => Assert.False(Coverage.CheckQuarter(false, null, 2026, 1, NOW).Fresh);

    [Fact]
    public void test_quarter_empty_after_deadline_is_permanent()
        => Assert.True(Coverage.CheckQuarter(false, NOW.AddDays(-10), 2026, 1, NOW).Fresh);

    [Fact]
    public void test_quarter_empty_in_window_rechecks_daily()
    {
        Assert.True(Coverage.CheckQuarter(false, NOW.AddHours(-2), 2026, 2, NOW).Fresh);
        Assert.False(Coverage.CheckQuarter(false, NOW.AddDays(-2), 2026, 2, NOW).Fresh);
    }

    // ── 宏观沉淀期 ──

    [Fact]
    public void test_macro_settled_after_60_days()
    {
        var w = Wm(D(2020, 1, 1), TODAY.AddDays(-90), 30 * 86400L);
        Assert.True(Coverage.CheckRange(w, "money_supply_month", D(2025, 1, 1), TODAY.AddDays(-90), NOW).Fresh);
    }

    [Fact]
    public void test_macro_recent_stale_weekly()
    {
        var w = Wm(D(2020, 1, 1), TODAY, 8 * 86400L);
        var d = Coverage.CheckRange(w, "deposit_rate", D(2026, 1, 1), TODAY, NOW);
        Assert.False(d.Fresh);
        Assert.Equal(TODAY.AddDays(-(8 + 59)), d.FetchRanges[0].Start);
    }

    [Fact]
    public void test_macro_recent_fresh_within_week()
    {
        var w = Wm(D(2020, 1, 1), TODAY, 86400);
        Assert.True(Coverage.CheckRange(w, "deposit_rate", D(2026, 1, 1), TODAY, NOW).Fresh);
    }

    // ── 交易日历（可请求未来） ──

    [Fact]
    public void test_calendar_future_not_clamped()
    {
        var w = Wm(D(2024, 1, 1), D(2026, 6, 30), 3600);
        var d = Coverage.CheckRange(w, "trade_calendar", D(2026, 1, 1), D(2026, 12, 31), NOW);
        Assert.Equal(Ranges((D(2026, 7, 1), D(2026, 12, 31))), d.FetchRanges);
    }

    [Fact]
    public void test_calendar_stale_refreshes_unsettled_and_merges_tail()
    {
        var w = Wm(D(2024, 1, 1), D(2026, 6, 30), 2 * 86400L);
        var d = Coverage.CheckRange(w, "trade_calendar", D(2026, 1, 1), D(2026, 12, 31), NOW);
        Assert.Equal(Ranges((TODAY.AddDays(-2), D(2026, 12, 31))), d.FetchRanges);
    }

    // ── 快照类 ──

    [Fact]
    public void test_snapshot_missing()
    {
        var d = Coverage.CheckSnapshot(null, "stock_list", TODAY, hasRows: false, NOW);
        Assert.Equal(Ranges((TODAY, TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_snapshot_historical_permanent()
        => Assert.True(Coverage.CheckSnapshot(null, "stock_list", TODAY.AddDays(-5), hasRows: true, NOW).Fresh);

    [Fact]
    public void test_snapshot_today_stale()
    {
        var w = Wm(TODAY, TODAY, 2 * 86400L);
        var d = Coverage.CheckSnapshot(w, "stock_list", TODAY, hasRows: true, NOW);
        Assert.Equal(Ranges((TODAY, TODAY)), d.FetchRanges);
    }

    [Fact]
    public void test_snapshot_today_fresh()
    {
        var w = Wm(TODAY, TODAY, 3600);
        Assert.True(Coverage.CheckSnapshot(w, "stock_list", TODAY, hasRows: true, NOW).Fresh);
    }

    // ── 写侧水位声明规则（claimable_last） ──

    [Fact]
    public void test_claim_empty_unsettled_tail_not_claimed()
        => Assert.Equal(TODAY.AddDays(-1), Coverage.ClaimableLast("k_d", TODAY, null, TODAY));

    [Fact]
    public void test_claim_actual_data_claims_through_actual()
        => Assert.Equal(TODAY, Coverage.ClaimableLast("k_d", TODAY, TODAY, TODAY));

    [Fact]
    public void test_claim_empty_settled_range_claimed()
        => Assert.Equal(D(2026, 6, 1), Coverage.ClaimableLast("k_d", D(2026, 6, 1), null, TODAY));

    [Fact]
    public void test_claim_weekly_caps_at_week_boundary()
        => Assert.Equal(D(2026, 6, 7), Coverage.ClaimableLast("k_w", TODAY, null, TODAY));

    [Fact]
    public void test_future_range_not_refetched()
    {
        var w = Wm(D(2020, 1, 1), TODAY, 60);
        Assert.True(Coverage.CheckRange(w, "k_d", D(2026, 6, 1), D(2099, 1, 31), NOW).Fresh);
        Assert.True(Coverage.CheckRange(w, "k_d", D(2099, 1, 1), D(2099, 1, 31), NOW).Fresh);
    }
}
