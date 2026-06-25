namespace StockData.Mcp.Data;

/// <summary>
/// 数据覆盖度/新鲜度判定——移植自 Python <c>server/db/coverage.py</c>（设计文档 5.4 节）。
///
/// 纯函数：输入水位（或 null）与请求范围，输出 <see cref="Decision"/>——直接读库（fresh）
/// 或给出需抓取的区间。不触碰数据库与时钟（now 由调用方注入，便于测试）。
///
/// 黄金标准 = Python <c>tests/test_coverage.py</c>，C# 侧逐 case 对齐（见 CoverageTests）。
/// 业务日期一律取中国时区（Asia/Shanghai），与 <c>core/timeutil.py</c> 一致。
/// </summary>
public static class Coverage
{
    // ── 刷新间隔（秒）：请求范围触及未定型区域时，超过该间隔需重抓 ──
    private const int Realtime = 300;
    private const int Daily = 86400;
    private const int Weekly = 604800;

    private static readonly IReadOnlyDictionary<string, int> RefreshIntervals = new Dictionary<string, int>
    {
        ["k_d"] = Realtime, ["k_w"] = Realtime, ["k_m"] = Realtime,
        ["k_5"] = Realtime, ["k_15"] = Realtime, ["k_30"] = Realtime, ["k_60"] = Realtime,
        ["adjust_factor"] = Realtime,
        ["stock_basic"] = Daily,
        ["dividend"] = Daily,
        ["profit"] = Daily, ["operation"] = Daily, ["growth"] = Daily,
        ["balance"] = Daily, ["cash_flow"] = Daily, ["dupont"] = Daily,
        ["express"] = Daily, ["forecast"] = Daily,
        ["trade_calendar"] = Daily,
        ["stock_list"] = Daily,
        ["index_sz50"] = Daily, ["index_hs300"] = Daily, ["index_zz500"] = Daily,
        ["industry"] = Weekly,
        ["deposit_rate"] = Weekly, ["loan_rate"] = Weekly, ["rrr"] = Weekly,
        ["money_supply_month"] = Weekly, ["money_supply_year"] = Weekly,
    };

    private static readonly HashSet<string> MinuteTypes = new() { "k_5", "k_15", "k_30", "k_60" };

    private static readonly HashSet<string> MacroSettled = new()
    {
        "deposit_rate", "loan_rate", "rrr", "money_supply_month", "money_supply_year",
    };

    /// <summary>首次触达的回填起点（上交所开市，设计文档 5.2.2）。</summary>
    private static readonly DateOnly AShareEpoch = new(1990, 12, 19);

    /// <summary>分钟线回填起点——镜像 <c>settings.minute_backfill_start</c>（默认 2023-01-01）。</summary>
    public static DateOnly MinuteBackfillStart { get; set; } = new(2023, 1, 1);

    private static readonly TimeZoneInfo Cst = TimeZoneInfo.FindSystemTimeZoneById("Asia/Shanghai");

    private static DateOnly CstDate(DateTimeOffset t)
        => DateOnly.FromDateTime(TimeZoneInfo.ConvertTime(t, Cst).DateTime);

    /// <summary>注入时刻对应的中国时区"今天"（写侧 ClaimableLast 等调用方使用）。</summary>
    public static DateOnly Today(DateTimeOffset now) => CstDate(now);

    private static DateOnly Min(DateOnly a, DateOnly b) => a < b ? a : b;
    private static DateOnly Max(DateOnly a, DateOnly b) => a > b ? a : b;

    // ISO weekday：Mon=0 … Sun=6（对齐 Python date.weekday()）
    private static int Weekday(DateOnly d) => ((int)d.DayOfWeek + 6) % 7;
    private static DateOnly Monday(DateOnly d) => d.AddDays(-Weekday(d));

    public static DateOnly BackfillStart(string dataType)
        => MinuteTypes.Contains(dataType) ? MinuteBackfillStart : AShareEpoch;

    /// <summary>该日期（含）之前的业务数据视为永久定型。</summary>
    public static DateOnly SettledBoundary(string dataType, DateOnly today) => dataType switch
    {
        "k_w" => Monday(today).AddDays(-1),                       // 本周一之前（周线按周定型）
        "k_m" => new DateOnly(today.Year, today.Month, 1).AddDays(-1), // 本月 1 日之前
        _ when MacroSettled.Contains(dataType) => today.AddDays(-60),  // 宏观 2 个月沉淀期
        _ => today.AddDays(-1),                                   // 默认：昨天及以前定型
    };

    /// <summary>
    /// 抓取完成后水位可声明的 last_date（写侧规则，fetcher 任务调用）。
    /// 已定型部分按请求范围声明；未定型尾部只认实际返回的最大业务日期——
    /// 数据源尚未发布的日期不声明覆盖，留待后续重抓（防永久空洞）。
    /// </summary>
    public static DateOnly ClaimableLast(string dataType, DateOnly requestedEnd, DateOnly? actualLast, DateOnly today)
    {
        var claimed = Min(requestedEnd, SettledBoundary(dataType, today));
        if (actualLast is DateOnly a && a > claimed) claimed = a;
        return claimed;
    }

    /// <summary>季度报告期：Q1→3/31、Q2→6/30、Q3→9/30、Q4→12/31。</summary>
    public static DateOnly QuarterEnd(int year, int quarter) => quarter switch
    {
        1 => new(year, 3, 31),
        2 => new(year, 6, 30),
        3 => new(year, 9, 30),
        4 => new(year, 12, 31),
        _ => throw new ArgumentOutOfRangeException(nameof(quarter)),
    };

    /// <summary>季报披露截止日：Q1→4/30、Q2→8/31、Q3→10/31、Q4→次年 4/30。</summary>
    public static DateOnly QuarterDisclosureDeadline(int year, int quarter) => quarter switch
    {
        1 => new(year, 4, 30),
        2 => new(year, 8, 31),
        3 => new(year, 10, 31),
        4 => new(year + 1, 4, 30),
        _ => throw new ArgumentOutOfRangeException(nameof(quarter)),
    };

    private static bool IsStale(Watermark wm, string dataType, DateTimeOffset now)
    {
        var age = (now - wm.LastFetchedAt).TotalSeconds;
        return age > RefreshIntervals[dataType];
    }

    /// <summary>
    /// 范围类数据集（K线/因子/快报预告/日历/宏观/分红折算区间）的判定。
    /// 返回的 FetchRanges 至多三段：头部缺口、尾部缺口、未定型区域刷新。
    /// </summary>
    public static Decision CheckRange(Watermark? wm, string dataType, DateOnly start, DateOnly end, DateTimeOffset now)
    {
        var today = CstDate(now);
        if (dataType != "trade_calendar")
        {
            // 除日历外业务数据不存在于未来：钳制请求尾，避免未来范围每次都判出尾部缺口
            if (end > today) end = today;
            if (end < start) return Decision.Covered("请求范围全部在未来，无可抓取数据");
        }

        if (wm is null)
        {
            // 首次触达：从回填起点抓到请求尾（保证覆盖连续）
            var fetchFrom = Min(BackfillStart(dataType), start);
            return new Decision(new[] { (fetchFrom, end) }, "首次触达，全量回填");
        }

        var ranges = new List<(DateOnly, DateOnly)>();
        var boundary = SettledBoundary(dataType, today);
        var stale = IsStale(wm, dataType, now);

        // 头部缺口（罕见：回填起点之前的请求）
        if (wm.FirstDate is DateOnly fd && start < fd)
            ranges.Add((start, fd.AddDays(-1)));

        // 尾部缺口。缺口完全落在未定型区时按刷新间隔节流（写侧 ClaimableLast 保证定型区之外
        // 只有实际拿到数据才声明覆盖，故这种缺口意味着"已请求过、数据源尚未发布"）。
        if (end > wm.LastDate)
        {
            var gapUnsettledOnly = dataType != "trade_calendar" && wm.LastDate >= boundary;
            if (!gapUnsettledOnly || stale)
                ranges.Add((wm.LastDate.AddDays(1), end));
        }

        // 未定型区域刷新：起点取**上次抓取时**的定型边界。上次抓取时尚未定型的数据
        // 即使现在已滑入定型区也要重新核实，否则会固化为陈旧 bar 或永久空洞。
        if (stale)
        {
            var fetchedDay = CstDate(wm.LastFetchedAt);
            var refreshFrom = Max(start, SettledBoundary(dataType, fetchedDay).AddDays(1));
            var refreshTo = dataType == "trade_calendar" ? end : Min(end, today);
            if (refreshFrom <= refreshTo) ranges.Add((refreshFrom, refreshTo));
        }

        return ranges.Count == 0
            ? Decision.Covered("已覆盖且新鲜")
            : new Decision(MergeRanges(ranges), "存在缺口或未定型数据过期");
    }

    /// <summary>
    /// 六类季度财报的判定。季度抓取点状，区间水位会虚假覆盖，故用两个点状事实：
    /// hasRows（事实表是否有该报告期行）、lastSuccess（最近一次成功抓取时刻，兼负结果记忆）。
    /// </summary>
    public static Decision CheckQuarter(bool hasRows, DateTimeOffset? lastSuccess, int year, int quarter, DateTimeOffset now)
    {
        var qEnd = QuarterEnd(year, quarter);
        var quarterRange = new[] { (new DateOnly(year, qEnd.Month - 2, 1), qEnd) };
        var deadline = QuarterDisclosureDeadline(year, quarter);
        var settled = CstDate(now) > deadline;

        if (hasRows)
        {
            if (settled) return Decision.Covered("披露截止日已过，永久有效");
            if (lastSuccess is DateTimeOffset ls0 && (now - ls0).TotalSeconds <= RefreshIntervals["profit"])
                return Decision.Covered("披露期内但仍新鲜");
            return new Decision(quarterRange, "披露期内数据过期");
        }

        // 无数据：区分"从未抓过"与"抓过确实没有"
        if (lastSuccess is not DateTimeOffset ls) return new Decision(quarterRange, "该季度未抓取过");
        if (settled && CstDate(ls) > deadline) return Decision.Covered("披露截止日后确认无数据，永久空结果");
        if ((now - ls).TotalSeconds <= RefreshIntervals["profit"]) return Decision.Covered("近期已查过，尚未披露");
        return new Decision(quarterRange, "可能已披露，重新检查");
    }

    /// <summary>
    /// 快照类数据集（股票列表/成分股/行业/基本信息）的判定。
    /// hasRows：事实表中该快照日是否已有数据（由调用方查询，保持本函数纯函数）。
    /// </summary>
    public static Decision CheckSnapshot(Watermark? wm, string dataType, DateOnly snapDate, bool hasRows, DateTimeOffset now)
    {
        var today = CstDate(now);
        var snapRange = new[] { (snapDate, snapDate) };

        if (!hasRows) return new Decision(snapRange, "快照不存在");
        if (snapDate < today) return Decision.Covered("历史快照永久有效");
        if (wm is null || IsStale(wm, dataType, now)) return new Decision(snapRange, "今日快照过期");
        return Decision.Covered("今日快照仍新鲜");
    }

    /// <summary>合并相邻（≤1 天间隔）/重叠区间，减少任务数。</summary>
    public static IReadOnlyList<(DateOnly Start, DateOnly End)> MergeRanges(List<(DateOnly Start, DateOnly End)> ranges)
    {
        var sorted = ranges.OrderBy(r => r.Start).ThenBy(r => r.End).ToList();
        var merged = new List<(DateOnly Start, DateOnly End)> { sorted[0] };
        foreach (var (s, e) in sorted.Skip(1))
        {
            var (lastS, lastE) = merged[^1];
            if (s <= lastE.AddDays(1))
                merged[^1] = (lastS, Max(lastE, e));
            else
                merged.Add((s, e));
        }
        return merged;
    }
}
