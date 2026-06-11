using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using StockData.Mcp.Indicators;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>技术指标分析工具（TA-Lib 本地计算，数据取自 REST 前复权 K 线）。</summary>
[McpServerToolType]
public static class IndicatorTools
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase, // Hit 等 record 与匿名对象风格统一
    };

    private static double? R(double? v) => v is { } x ? Math.Round(x, 4) : null;

    [McpServerTool(Name = "calc_indicators")]
    [Description("计算技术指标（基于前复权日K线）。indicators 为逗号分隔列表，支持 MA{n}/EMA{n}/RSI{n}/ATR{n}/CCI{n}/MACD/KDJ/BOLL/OBV，如 \"MA5,MA20,MACD,RSI14\"。output=series 返回逐日序列，latest 只返回最新值。")]
    public static async Task<string> CalcIndicators(
        StockDataApiClient api,
        [Description("股票代码")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("指标列表，逗号分隔，如 MA5,MA20,MACD,RSI14,KDJ,BOLL")] string indicators = "MA5,MA20,MACD,RSI14",
        [Description("series 逐日序列 / latest 最新值")] string output = "series",
        [Description("复权方式：2前复权(默认)/1后复权/3不复权")] string adjust_flag = "2",
        [Description("series 模式最大返回行数")] int limit = 120,
        CancellationToken ct = default)
    {
        var (specs, err) = IndicatorEngine.Parse(indicators);
        if (err != null) return err;

        var lookback = IndicatorEngine.MaxLookback(specs);
        var (series, loadErr) = await KlineLoader.LoadAsync(api, code, start_date, end_date,
            lookback, adjust_flag, ct);
        if (series is null) return loadErr!;

        var columns = new Dictionary<string, double?[]>();
        foreach (var spec in specs)
            foreach (var (name, values) in IndicatorEngine.Compute(spec, series))
                columns[name] = values;

        var from = series.IndexOf(start_date);
        if (from >= series.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var last = series.Length - 1;
            var latest = new Dictionary<string, object?>
            {
                ["code"] = code, ["date"] = series.Dates[last],
                ["close"] = R(series.Close[last]),
            };
            foreach (var (name, values) in columns) latest[name] = R(values[last]);
            return JsonSerializer.Serialize(latest, JsonOpts);
        }

        var rows = new List<Dictionary<string, object?>>();
        for (var i = from; i < series.Length; i++)
        {
            var row = new Dictionary<string, object?>
            {
                ["date"] = series.Dates[i], ["close"] = R(series.Close[i]),
            };
            foreach (var (name, values) in columns) row[name] = R(values[i]);
            rows.Add(row);
        }
        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList(); // 序列截断保尾部（最新优先）
        var body = JsonSerializer.Serialize(new { code, adjust_flag, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    [McpServerTool(Name = "detect_candlestick_patterns")]
    [Description("K线形态识别（十字星/锤子线/吞没/早晨之星/三只乌鸦等 15 种常用形态），返回命中日期与方向（bullish/bearish）。patterns 为空时检测全部形态。")]
    public static async Task<string> DetectCandlestickPatterns(
        StockDataApiClient api,
        [Description("股票代码")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("形态列表，逗号分隔（可选）：Doji/Hammer/Engulfing/MorningStar/EveningStar/ThreeWhiteSoldiers/ThreeBlackCrows/...")] string? patterns = null,
        CancellationToken ct = default)
    {
        string[]? names = null;
        if (!string.IsNullOrWhiteSpace(patterns))
        {
            names = patterns.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            var unknown = names.Where(n => !PatternEngine.Patterns.ContainsKey(n)).ToList();
            if (unknown.Count > 0)
                return $"Error: 未知形态 {string.Join(",", unknown)}。可选: {string.Join("/", PatternEngine.Patterns.Keys)}";
        }

        var (series, loadErr) = await KlineLoader.LoadAsync(api, code, start_date, end_date,
            PatternEngine.MaxLookback, "2", ct);
        if (series is null) return loadErr!;

        var from = series.IndexOf(start_date);
        var hits = PatternEngine.Detect(series, names, from);
        return JsonSerializer.Serialize(new { code, total = hits.Count, hits }, JsonOpts);
    }

    [McpServerTool(Name = "technical_summary")]
    [Description("技术面综合信号面板：均线多空排列、MACD金叉死叉、RSI/KDJ超买超卖、布林带位置、量能变化，输出结构化结论。")]
    public static async Task<string> TechnicalSummary(
        StockDataApiClient api,
        [Description("股票代码")] string code,
        [Description("分析基准日 YYYY-MM-DD，缺省最新交易日")] string? date = null,
        CancellationToken ct = default)
    {
        var end = date ?? DateTime.Today.ToString("yyyy-MM-dd");
        var start = DateTime.Parse(end).AddDays(-30).ToString("yyyy-MM-dd");
        var (specs, _) = IndicatorEngine.Parse("MA5,MA10,MA20,MA60,MACD,RSI14,KDJ,BOLL");
        var (k, loadErr) = await KlineLoader.LoadAsync(api, code, start, end,
            IndicatorEngine.MaxLookback(specs), "2", ct);
        if (k is null) return loadErr!;

        var c = new Dictionary<string, double?[]>();
        foreach (var spec in specs)
            foreach (var (name, values) in IndicatorEngine.Compute(spec, k))
                c[name] = values;

        var i = k.Length - 1;
        double? V(string name) => c[name][i];
        var close = k.Close[i];

        // 均线排列
        var (ma5, ma10, ma20, ma60) = (V("MA5"), V("MA10"), V("MA20"), V("MA60"));
        var maTrend = (ma5, ma10, ma20, ma60) switch
        {
            ({ } a, { } b, { } x, { } y) when a > b && b > x && x > y => "多头排列",
            ({ } a, { } b, { } x, { } y) when a < b && b < x && x < y => "空头排列",
            _ => "纠缠/无明确排列",
        };

        // MACD 最近 5 根内的金叉/死叉
        string macdCross = "无";
        for (var j = Math.Max(1, i - 4); j <= i; j++)
        {
            var (difPrev, deaPrev) = (c["MACD_DIF"][j - 1], c["MACD_DEA"][j - 1]);
            var (dif, dea) = (c["MACD_DIF"][j], c["MACD_DEA"][j]);
            if (difPrev is { } dp && deaPrev is { } ep && dif is { } d && dea is { } e2)
            {
                if (dp <= ep && d > e2) macdCross = $"金叉（{k.Dates[j]}）";
                if (dp >= ep && d < e2) macdCross = $"死叉（{k.Dates[j]}）";
            }
        }

        var rsi = V("RSI14");
        var kdjJ = V("KDJ_J");
        var (bollUp, bollLow) = (V("BOLL_UPPER"), V("BOLL_LOWER"));
        var bollPos = (bollUp, bollLow) switch
        {
            ({ } u, _) when close > u => "上轨之上（超强/超买）",
            (_, { } l) when close < l => "下轨之下（超弱/超卖）",
            ({ } u, { } l) => $"带内位置 {Math.Round((close - l) / (u - l) * 100, 1)}%",
            _ => "数据不足",
        };
        var vol5 = k.Volume.Skip(Math.Max(0, i - 4)).Take(5).Average();
        var vol10 = k.Volume.Skip(Math.Max(0, i - 9)).Take(10).Average();

        var summary = new
        {
            code,
            date = k.Dates[i],
            close = R(close),
            ma = new { ma5 = R(ma5), ma10 = R(ma10), ma20 = R(ma20), ma60 = R(ma60), 排列 = maTrend },
            macd = new { dif = R(V("MACD_DIF")), dea = R(V("MACD_DEA")), hist = R(V("MACD_HIST")), 近5日交叉 = macdCross },
            rsi14 = new { value = R(rsi), 状态 = rsi switch { null => "数据不足", > 70 => "超买", < 30 => "超卖", _ => "中性" } },
            kdj = new { k = R(V("KDJ_K")), d = R(V("KDJ_D")), j = R(kdjJ), 状态 = kdjJ switch { null => "数据不足", > 100 => "超买", < 0 => "超卖", _ => "中性" } },
            boll = new { upper = R(bollUp), middle = R(V("BOLL_MIDDLE")), lower = R(bollLow), 位置 = bollPos },
            volume = new { 量比5v10 = Math.Round(vol5 / vol10, 2), 解读 = vol5 > vol10 * 1.2 ? "放量" : vol5 < vol10 * 0.8 ? "缩量" : "平量" },
        };
        return JsonSerializer.Serialize(summary, JsonOpts);
    }

    [McpServerTool(Name = "compare_indicators")]
    [Description("多标的同指标横向对比：对一组股票计算同一指标的最新值并排序。")]
    public static async Task<string> CompareIndicators(
        StockDataApiClient api,
        [Description("股票代码列表，逗号分隔，如 600000,600519,000001")] string codes,
        [Description("单个指标，如 RSI14/MA20/CCI14")] string indicator = "RSI14",
        [Description("基准日 YYYY-MM-DD，缺省最新")] string? date = null,
        CancellationToken ct = default)
    {
        var (specs, err) = IndicatorEngine.Parse(indicator);
        if (err != null) return err;
        if (specs.Count != 1) return "Error: compare_indicators 只支持单个指标";
        var spec = specs[0];

        var end = date ?? DateTime.Today.ToString("yyyy-MM-dd");
        var start = DateTime.Parse(end).AddDays(-10).ToString("yyyy-MM-dd");
        var codeList = codes.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (codeList.Length is 0 or > 20) return "Error: 代码数量须在 1-20 之间";

        var tasks = codeList.Select(async c =>
        {
            var (k, e) = await KlineLoader.LoadAsync(api, c, start, end,
                IndicatorEngine.Lookback(spec), "2", ct);
            if (k is null) return new { code = c, date = (string?)null, value = (double?)null, error = (string?)e };
            var values = IndicatorEngine.Compute(spec, k).First().Value;
            var i = k.Length - 1;
            return new { code = c, date = (string?)k.Dates[i], value = R(values[i]), error = (string?)null };
        });
        var results = (await Task.WhenAll(tasks))
            .OrderByDescending(r => r.value ?? double.MinValue).ToList();
        return JsonSerializer.Serialize(new { indicator = spec.Key, results }, JsonOpts);
    }
}
