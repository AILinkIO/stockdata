using System.ComponentModel;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using ModelContextProtocol.Server;
using StockData.Mcp.Indicators;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>
/// 双均线（Dual MA）趋势系统，固定使用 EMA。
///
/// 结构：
///   快线 EMA(fast_period)  —— 默认 20，约一个月交易日，反映波段动能
///   慢线 EMA(slow_period)  —— 默认 50，约一个季度，反映中期趋势中枢
///
/// 信号：
///   金叉 —— 快线上穿慢线，中期趋势转多确认
///   死叉 —— 快线下穿慢线，中期趋势转空确认
///   spread（快线−慢线）扩大 = 趋势加速，收窄 = 趋势衰竭、临近交叉
///
/// 注意：双均线为滞后指标，震荡市中两线缠绕会产生频繁假信号，
/// 建议结合 RSI/CCI 过滤（本服务已提供 get_rsi）。
/// </summary>
[McpServerToolType]
public static class DualMaTools
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static double? R(double? v) => v is { } x ? Math.Round(x, 4) : null;

    [McpServerTool(Name = "get_dual_ma")]
    [Description(
        "双均线趋势系统（EMA）：快线默认 EMA20、慢线默认 EMA50。" +
        "输出 fast/slow（两条均线）、spread（快−慢，趋势强度）、" +
        "cross（金叉/死叉，仅交叉当日标注）、trend（多头排列/空头排列）。" +
        "output=series 返回逐日序列，latest 返回最新值。" +
        "adjust_flag: 2前复权(默认)/1后复权/3不复权。")]
    public static async Task<string> GetDualMa(
        StockDataApiClient api,
        IMemoryCache cache,
        [Description("股票代码，如 sh.600000")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("快线 EMA 周期，须小于慢线")] int fast_period = 20,
        [Description("慢线 EMA 周期")] int slow_period = 50,
        [Description("复权：2前复权/1后复权/3不复权")] string adjust_flag = "2",
        [Description("series 逐日序列 / latest 最新值")] string output = "series",
        [Description("series 模式最大返回行数")] int limit = 120,
        CancellationToken ct = default)
    {
        if (fast_period < 2)
            return "Error: fast_period 须 >= 2";
        if (slow_period <= fast_period)
            return $"Error: slow_period({slow_period}) 须大于 fast_period({fast_period})";

        // +1：交叉检测需要 start_date 前一根 K 线的有效均线值
        var lookback = TalibComputer.EmaLookback(slow_period) + 1;
        var (k, err) = await KlineLoader.LoadAsync(api, cache, code,
            start_date, end_date, lookback, adjust_flag, ct);
        if (k is null) return err!;

        var fast = TalibComputer.Ema(k.Close, fast_period);
        var slow = TalibComputer.Ema(k.Close, slow_period);

        var from = k.IndexOf(start_date);
        if (from >= k.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var i = k.Length - 1;
            return JsonSerializer.Serialize(new
            {
                code,
                fast_period,
                slow_period,
                date   = k.Dates[i],
                close  = R(k.Close[i]),
                fast   = R(fast[i]),
                slow   = R(slow[i]),
                spread = Spread(fast[i], slow[i]),
                cross  = i > 0 ? Cross(fast[i - 1], slow[i - 1], fast[i], slow[i]) : null,
                trend  = Trend(fast[i], slow[i]),
            }, JsonOpts);
        }

        var rows = Enumerable.Range(from, k.Length - from)
            .Select(i => (object)new
            {
                date   = k.Dates[i],
                close  = R(k.Close[i]),
                fast   = R(fast[i]),
                slow   = R(slow[i]),
                spread = Spread(fast[i], slow[i]),
                cross  = i > 0 ? Cross(fast[i - 1], slow[i - 1], fast[i], slow[i]) : null,
                trend  = Trend(fast[i], slow[i]),
            })
            .ToList();

        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList();
        var body = JsonSerializer.Serialize(
            new { code, fast_period, slow_period, adjust_flag, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    private static double? Spread(double? fast, double? slow) =>
        fast is { } f && slow is { } s ? Math.Round(f - s, 4) : null;

    /// <summary>
    /// 交叉检测：前一根快线在慢线下方（含相等）且当前上穿为金叉，反之为死叉。
    /// 任一值处于 warmup（null）时不判定。
    /// </summary>
    internal static string? Cross(double? prevFast, double? prevSlow, double? fast, double? slow)
    {
        if (prevFast is null || prevSlow is null || fast is null || slow is null) return null;
        if (prevFast <= prevSlow && fast > slow) return "金叉";
        if (prevFast >= prevSlow && fast < slow) return "死叉";
        return null;
    }

    /// <summary>快线在慢线上方为多头排列，下方为空头排列，相等为均线粘合。</summary>
    internal static string? Trend(double? fast, double? slow)
    {
        if (fast is null || slow is null) return null;
        if (fast > slow) return "多头排列";
        if (fast < slow) return "空头排列";
        return "均线粘合";
    }
}
