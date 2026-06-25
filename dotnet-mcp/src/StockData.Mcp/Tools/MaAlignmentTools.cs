using System.ComponentModel;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using ModelContextProtocol.Server;
using StockData.Mcp.Indicators;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>
/// 均线多头排列（MA Alignment）趋势系统，基于多条 SMA。
///
/// 结构：
///   默认周期 [5, 10, 20, 60]，分别代表短线 / 波段 / 中线 / 长线市场成本。
///   调用方可自定义周期数组（须严格升序、至少 2 个、每个 >= 2）。
///   多头排列：MA(短) > MA(中) > MA(长)，数值严格递减；
///   空头排列：MA(短) &lt; MA(中) &lt; MA(长)，数值严格递增；
///   其余视为"未排列"（缠绕、粘合、交叉中段）。
///
/// 信号：
///   多头形成 —— 上一日非多头排列，当日转为多头排列（趋势启动确认）
///   多头破坏 —— 上一日多头排列，当日不再多头排列（趋势瓦解，注意风险）
///   空头形成 / 空头破坏 —— 空头对称版本
///
/// 注意：均线为滞后指标，震荡市中均线缠绕会产生频繁"未排列"或假信号，
/// 建议结合 RSI/CCI 与量能过滤（本服务已提供 get_rsi / calc_indicators）。
/// </summary>
[McpServerToolType]
public static class MaAlignmentTools
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static readonly int[] DefaultPeriods = [5, 10, 20, 60];

    private static double? R(double? v) => v is { } x ? Math.Round(x, 4) : null;

    [McpServerTool(Name = "get_ma_alignment")]
    [Description(
        "均线多头排列（MA Alignment）：对一组 SMA 周期（默认 5/10/20/60）判断每日排列状态，" +
        "输出每条 MA 值、alignment（多头排列/空头排列/未排列）、signal（多头形成/多头破坏/空头形成/空头破坏）。" +
        "output=series 返回逐日序列，latest 返回最新值。" +
        "periods 须严格升序、至少 2 个、每个 >= 2；adjust_flag: 2前复权(默认)/1后复权/3不复权。" +
        "frequency: d日(默认)/w周/m月（在对应周期K线上计算）。")]
    public static async Task<string> GetMaAlignment(
        StockDataApiClient api,
        IMemoryCache cache,
        StockData.Mcp.Data.KlineReadService pipeline,
        [Description("股票代码，如 sh.600000")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("SMA 周期数组，默认 [5,10,20,60]，须严格升序、>=2")] int[]? periods = null,
        [Description("复权：2前复权/1后复权/3不复权")] string adjust_flag = "2",
        [Description("K线周期：d日(默认)/w周/m月")] string frequency = "d",
        [Description("series 逐日序列 / latest 最新值")] string output = "series",
        [Description("series 模式最大返回行数")] int limit = 120,
        CancellationToken ct = default)
    {
        periods ??= DefaultPeriods;

        if (periods.Length < 2)
            return $"Error: periods 至少需要 2 个（当前 {periods.Length} 个）";
        for (var i = 1; i < periods.Length; i++)
        {
            if (periods[i] <= periods[i - 1])
                return $"Error: periods 须严格升序，第 {i} 个 ({periods[i]}) 不大于第 {i - 1} 个 ({periods[i - 1]})";
        }
        if (periods[0] < 2)
            return $"Error: periods 每个值须 >= 2（当前最小 {periods[0]}）";

        // +1：signal 检测需要 start_date 前一根 K 线的有效 MA 值
        var lookback = TalibComputer.SmaLookback(periods[^1]) + 1;
        var (k, err) = await KlineLoader.LoadAsync(api, pipeline, cache, code,
            start_date, end_date, lookback, adjust_flag, frequency, ct);
        if (k is null) return err!;

        // 计算所有周期的 SMA
        var maArrays = new double?[periods.Length][];
        for (var p = 0; p < periods.Length; p++)
            maArrays[p] = TalibComputer.Sma(k.Close, periods[p]);

        var from = k.IndexOf(start_date);
        if (from >= k.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var i = k.Length - 1;
            var current = CollectValues(maArrays, i);
            var prev = i > 0 ? CollectValues(maArrays, i - 1) : current;
            var row = new Dictionary<string, object?>
            {
                ["code"]     = code,
                ["periods"]  = periods,
                ["date"]     = k.Dates[i],
                ["close"]    = R(k.Close[i]),
                ["alignment"] = Alignment(current),
                ["signal"]    = i > 0 ? Signal(prev, current) : null,
            };
            for (var p = 0; p < periods.Length; p++)
                row[$"ma{periods[p]}"] = R(maArrays[p][i]);
            return JsonSerializer.Serialize(row, JsonOpts);
        }

        var rows = Enumerable.Range(from, k.Length - from)
            .Select(i =>
            {
                var current = CollectValues(maArrays, i);
                var prev = i > 0 ? CollectValues(maArrays, i - 1) : current;
                var row = new Dictionary<string, object?>
                {
                    ["date"]      = k.Dates[i],
                    ["close"]     = R(k.Close[i]),
                    ["alignment"] = Alignment(current),
                    ["signal"]    = i > 0 ? Signal(prev, current) : null,
                };
                for (var p = 0; p < periods.Length; p++)
                    row[$"ma{periods[p]}"] = R(maArrays[p][i]);
                return (object)row;
            })
            .ToList();

        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList();
        var body = JsonSerializer.Serialize(
            new { code, periods, adjust_flag, frequency, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    private static double?[] CollectValues(double?[][] maArrays, int i)
    {
        var values = new double?[maArrays.Length];
        for (var p = 0; p < maArrays.Length; p++)
            values[p] = maArrays[p][i];
        return values;
    }

    /// <summary>
    /// 判断均线排列状态。
    /// 多头排列：所有均线严格递减（短期 &gt; 长期），即 ma[0] &gt; ma[1] &gt; ... &gt; ma[n-1]
    /// 空头排列：所有均线严格递增（短期 &lt; 长期）
    /// 任一为 null（warmup）返回 null。
    /// </summary>
    internal static string? Alignment(double?[] values)
    {
        if (values.Any(v => v is null)) return null;
        var allDecreasing = true;
        var allIncreasing = true;
        for (var i = 1; i < values.Length; i++)
        {
            if (!(values[i - 1] > values[i])) allDecreasing = false;
            if (!(values[i - 1] < values[i])) allIncreasing = false;
        }
        if (allDecreasing) return "多头排列";
        if (allIncreasing) return "空头排列";
        return "未排列";
    }

    /// <summary>
    /// 检测排列状态变化信号。
    /// 多头形成：前一日非多头排列，当日转为多头排列
    /// 多头破坏：前一日多头排列，当日不再多头排列
    /// 空头形成 / 空头破坏：空头对称版本。
    /// 任一日的排列状态为 null（warmup）时不判定。
    /// </summary>
    internal static string? Signal(double?[] prevValues, double?[] values)
    {
        var prev = Alignment(prevValues);
        var curr = Alignment(values);
        if (prev is null || curr is null) return null;
        if (prev != "多头排列" && curr == "多头排列") return "多头形成";
        if (prev == "多头排列" && curr != "多头排列") return "多头破坏";
        if (prev != "空头排列" && curr == "空头排列") return "空头形成";
        if (prev == "空头排列" && curr != "空头排列") return "空头破坏";
        return null;
    }
}