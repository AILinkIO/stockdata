using System.ComponentModel;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using ModelContextProtocol.Server;
using StockData.Mcp.Indicators;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>
/// Vegas Channel（Vegas 通道）多 EMA 趋势系统。
///
/// 结构：
///   快速信号线  EMA12 / EMA21          —— 短期动量，交叉给出买卖方向
///   通道 1      EMA144 / EMA169         —— 短中期支撑/压力带
///   中轴参考    EMA233                  —— 斐波那契中轴
///   通道 2      EMA576 / EMA676         —— 长期支撑/压力带
///
/// 周期全部使用原始定义的固定值（斐波那契数列）。
/// EMA676 需要 675 根预热 K 线（约 3 年日线），首次请求加载量较大，命中缓存后瞬时返回。
/// </summary>
[McpServerToolType]
public static class VegasChannelTools
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    // Vegas Channel 固定周期
    private static readonly int[] Periods = [12, 21, 144, 169, 233, 576, 676];

    private static double? R(double? v) => v is { } x ? Math.Round(x, 4) : null;

    [McpServerTool(Name = "get_vegas_channel")]
    [Description(
        "Vegas 通道：EMA12/21（快速信号线）+ EMA144/169（通道1）+ EMA233（中轴）+ EMA576/676（通道2）。" +
        "zone 字段综合价格位置与快线方向：多头强势/多头/回调整理/通道1内/通道2内/弱势反弹/空头。" +
        "output=series 返回逐日序列，latest 返回最新值。" +
        "注：EMA676 需约 3 年日线预热，首次请求耗时较长。adjust_flag: 2前复权(默认)/1后复权/3不复权。" +
        "frequency: d日(默认)/w周/m月（在对应周期K线上计算；周/月线 EMA676 所需历史极长，慎用）。")]
    public static async Task<string> GetVegasChannel(
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
        // EMA676 决定最大预热量
        var lookback = TalibComputer.EmaLookback(676);
        var (k, err) = await KlineLoader.LoadAsync(api, pipeline, cache, code,
            start_date, end_date, lookback, adjust_flag, frequency, ct);
        if (k is null) return err!;

        // 计算全部 7 条 EMA
        var ema12  = TalibComputer.Ema(k.Close, 12);
        var ema21  = TalibComputer.Ema(k.Close, 21);
        var ema144 = TalibComputer.Ema(k.Close, 144);
        var ema169 = TalibComputer.Ema(k.Close, 169);
        var ema233 = TalibComputer.Ema(k.Close, 233);
        var ema576 = TalibComputer.Ema(k.Close, 576);
        var ema676 = TalibComputer.Ema(k.Close, 676);

        var from = k.IndexOf(start_date);
        if (from >= k.Length) return $"Error: {code} 在 {start_date}~{end_date} 无K线数据";

        if (output.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            var i = k.Length - 1;
            var close = k.Close[i];
            return JsonSerializer.Serialize(new
            {
                code,
                date   = k.Dates[i],
                close  = R(close),
                ema12  = R(ema12[i]),
                ema21  = R(ema21[i]),
                ema144 = R(ema144[i]),
                ema169 = R(ema169[i]),
                ema233 = R(ema233[i]),
                ema576 = R(ema576[i]),
                ema676 = R(ema676[i]),
                zone   = Zone(close, ema12[i], ema21[i], ema144[i], ema169[i], ema576[i], ema676[i]),
            }, JsonOpts);
        }

        var rows = Enumerable.Range(from, k.Length - from)
            .Select(i =>
            {
                var close = k.Close[i];
                return (object)new
                {
                    date   = k.Dates[i],
                    close  = R(close),
                    ema12  = R(ema12[i]),
                    ema21  = R(ema21[i]),
                    ema144 = R(ema144[i]),
                    ema169 = R(ema169[i]),
                    ema233 = R(ema233[i]),
                    ema576 = R(ema576[i]),
                    ema676 = R(ema676[i]),
                    zone   = Zone(close, ema12[i], ema21[i], ema144[i], ema169[i], ema576[i], ema676[i]),
                };
            })
            .ToList();

        var total = rows.Count;
        if (total > limit) rows = rows.TakeLast(limit).ToList();
        var body = JsonSerializer.Serialize(new { code, adjust_flag, frequency, rows }, JsonOpts);
        return total > limit ? $"{body}\n（共 {total} 行，已截断为最近 {limit} 行）" : body;
    }

    /// <summary>
    /// 综合价格位置（相对两条通道）与快线方向（EMA12 vs EMA21）判断当前市场区域。
    ///
    /// 区域优先级（从强势多头到强势空头）：
    ///   多头强势  —— 价格在通道2上方，且 EMA12 > EMA21
    ///   多头      —— 价格在通道1上方（可能低于通道2），且 EMA12 > EMA21
    ///   回调整理  —— 价格在通道1上方，但 EMA12 &lt;= EMA21（多头结构，动量转弱）
    ///   通道1内   —— 价格在 EMA144~EMA169 带内（短期支撑/压力震荡区）
    ///   通道2内   —— 价格在 EMA576~EMA676 带内（长期支撑/压力震荡区）
    ///   弱势反弹  —— 价格在通道1下方，但 EMA12 > EMA21（空头结构内的技术性反弹）
    ///   空头      —— 价格在通道1下方，且 EMA12 &lt;= EMA21
    /// </summary>
    private static string Zone(
        double close,
        double? e12, double? e21,
        double? e144, double? e169,
        double? e576, double? e676)
    {
        if (e144 is null || e169 is null) return "数据不足";

        var bull = e12 > e21;

        // 通道2上方
        if (e676 is not null && close > e676) return bull ? "多头强势" : "多头震荡";

        // 通道2内
        if (e576 is not null && e676 is not null && close >= e576 && close <= e676)
            return "通道2内";

        // 通道1上方（通道2下方）
        if (close > e169) return bull ? "多头" : "回调整理";

        // 通道1内
        if (close >= e144 && close <= e169) return "通道1内";

        // 通道1下方
        return bull ? "弱势反弹" : "空头";
    }
}
