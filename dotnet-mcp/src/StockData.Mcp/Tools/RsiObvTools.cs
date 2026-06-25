using System.ComponentModel;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using ModelContextProtocol.Server;
using StockData.Mcp.Indicators;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>RSI 与 OBV 指标工具（TALib 本地计算，K线取自后端日/周/月线）。</summary>
[McpServerToolType]
public static class RsiObvTools
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static double? R4(double? v) => v is { } x ? Math.Round(x, 4) : null;
    private static double? R0(double? v) => v is { } x ? Math.Round(x, 0) : null;

    // ── RSI ──────────────────────────────────────────────────────────

    [McpServerTool(Name = "get_rsi")]
    [Description("计算股票 RSI（相对强弱指数）。output=series 返回逐日序列，latest 只返回最新值及超买超卖判断。adjust_flag: 2前复权(默认)/1后复权/3不复权。frequency: d日(默认)/w周/m月（在对应周期K线上计算）。")]
    public static async Task<string> GetRsi(
        StockDataApiClient api,
        IMemoryCache cache,
        StockData.Mcp.Data.KlineReadService pipeline,
        [Description("股票代码，如 sh.600000")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("RSI 周期，默认 14")] int period = 14,
        [Description("复权：2前复权/1后复权/3不复权")] string adjust_flag = "2",
        [Description("K线周期：d日(默认)/w周/m月")] string frequency = "d",
        [Description("series 逐日序列 / latest 最新值")] string output = "series",
        [Description("series 模式最大返回行数")] int limit = 120,
        CancellationToken ct = default)
    {
        if (period is < 2 or > 200) return "Error: period 须在 2-200 之间";

        var lookback = TalibComputer.RsiLookback(period);
        var (k, err) = await KlineLoader.LoadAsync(api, pipeline, cache, code,
            start_date, end_date, lookback, adjust_flag, frequency, ct);
        if (k is null) return err!;

        var rsi = TalibComputer.Rsi(k.Close, period);
        var from = k.IndexOf(start_date);
        if (from >= k.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var i = k.Length - 1;
            var val = R4(rsi[i]);
            return JsonSerializer.Serialize(new
            {
                code, date = k.Dates[i], close = R4(k.Close[i]),
                rsi = val, period,
                signal = val switch { > 70 => "超买", < 30 => "超卖", not null => "中性", _ => "数据不足" }
            }, JsonOpts);
        }

        var rows = Enumerable.Range(from, k.Length - from)
            .Select(i => (object)new { date = k.Dates[i], close = R4(k.Close[i]), rsi = R4(rsi[i]) })
            .ToList();

        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList();
        var body = JsonSerializer.Serialize(new { code, period, adjust_flag, frequency, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    // ── OBV ──────────────────────────────────────────────────────────

    [McpServerTool(Name = "get_obv")]
    [Description("计算股票 OBV（能量潮）。OBV 累积量价方向，上涨日加量、下跌日减量，衡量资金流向趋势与价格背离。adjust_flag: 2前复权(默认)/1后复权/3不复权。frequency: d日(默认)/w周/m月（在对应周期K线上计算）。")]
    public static async Task<string> GetObv(
        StockDataApiClient api,
        IMemoryCache cache,
        StockData.Mcp.Data.KlineReadService pipeline,
        [Description("股票代码，如 sh.600000")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("复权：2前复权/1后复权/3不复权")] string adjust_flag = "2",
        [Description("K线周期：d日(默认)/w周/m月")] string frequency = "d",
        [Description("series 逐日序列 / latest 最新值")] string output = "series",
        [Description("series 模式最大返回行数")] int limit = 120,
        CancellationToken ct = default)
    {
        // OBV 无需预热 bar（lookback=0），但为确保起始基准一致，向前多取 5 根
        var (k, err) = await KlineLoader.LoadAsync(api, pipeline, cache, code,
            start_date, end_date, 5, adjust_flag, frequency, ct);
        if (k is null) return err!;

        var obv = TalibComputer.Obv(k.Close, k.Volume);
        var from = k.IndexOf(start_date);
        if (from >= k.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var i = k.Length - 1;
            return JsonSerializer.Serialize(new
            {
                code, date = k.Dates[i], close = R4(k.Close[i]),
                volume = (long)k.Volume[i], obv = R0(obv[i]),
            }, JsonOpts);
        }

        var rows = Enumerable.Range(from, k.Length - from)
            .Select(i => (object)new
            {
                date = k.Dates[i], close = R4(k.Close[i]),
                volume = (long)k.Volume[i], obv = R0(obv[i]),
            })
            .ToList();

        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList();
        var body = JsonSerializer.Serialize(new { code, adjust_flag, frequency, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    // ── 双周期 CCI ───────────────────────────────────────────────────

    // 快线/慢线周期（斐波那契数列，与富途牛牛双周期 CCI 系统一致）
    private const int CciFast = 55;
    private const int CciSlow = 144;

    [McpServerTool(Name = "get_cci")]
    [Description("双周期 CCI 动量系统（CCI55 快线 + CCI144 慢线 + DIFF 差值柱）。" +
                 "zone 字段：强势多头/强势空头（CCI144 突破±100）、短期超买/短期超卖（CCI55 突破±100 而 CCI144 未确认）、中性。" +
                 "output=series 返回逐日序列，latest 只返回最新值。adjust_flag: 2前复权(默认)/1后复权/3不复权。frequency: d日(默认)/w周/m月（在对应周期K线上计算）。")]
    public static async Task<string> GetCci(
        StockDataApiClient api,
        IMemoryCache cache,
        StockData.Mcp.Data.KlineReadService pipeline,
        [Description("股票代码，如 sh.600000")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("复权：2前复权/1后复权/3不复权")] string adjust_flag = "2",
        [Description("K线周期：d日(默认)/w周/m月")] string frequency = "d",
        [Description("series 逐日序列 / latest 最新值")] string output = "series",
        [Description("series 模式最大返回行数")] int limit = 120,
        CancellationToken ct = default)
    {
        var lookback = TalibComputer.CciLookback(CciSlow); // 慢线决定所需预热 bar 数
        var (k, err) = await KlineLoader.LoadAsync(api, pipeline, cache, code,
            start_date, end_date, lookback, adjust_flag, frequency, ct);
        if (k is null) return err!;

        var fast = TalibComputer.Cci(k.High, k.Low, k.Close, CciFast);
        var slow = TalibComputer.Cci(k.High, k.Low, k.Close, CciSlow);
        var from = k.IndexOf(start_date);
        if (from >= k.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var i = k.Length - 1;
            var f = R4(fast[i]);
            var s = R4(slow[i]);
            var diff = f is not null && s is not null ? Math.Round(f.Value - s.Value, 4) : (double?)null;
            return JsonSerializer.Serialize(new
            {
                code, date = k.Dates[i], close = R4(k.Close[i]),
                cci55 = f, cci144 = s, diff,
                zone = Zone(fast[i], slow[i]),
            }, JsonOpts);
        }

        var rows = Enumerable.Range(from, k.Length - from)
            .Select(i =>
            {
                var f = R4(fast[i]);
                var s = R4(slow[i]);
                var diff = f is not null && s is not null ? Math.Round(f.Value - s.Value, 4) : (double?)null;
                return (object)new { date = k.Dates[i], close = R4(k.Close[i]), cci55 = f, cci144 = s, diff, zone = Zone(fast[i], slow[i]) };
            })
            .ToList();

        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList();
        var body = JsonSerializer.Serialize(new { code, adjust_flag, fast_period = CciFast, slow_period = CciSlow, frequency, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    private static string Zone(double? fast, double? slow) => (fast, slow) switch
    {
        (_, > 100)                              => "强势多头",
        (_, < -100)                             => "强势空头",
        (> 100, >= -100 and <= 100)             => "短期超买",
        (< -100, >= -100 and <= 100)            => "短期超卖",
        (not null, not null)                    => "中性",
        _                                       => "数据不足",
    };
}
