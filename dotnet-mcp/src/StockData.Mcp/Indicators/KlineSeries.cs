using System.Text.Json;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Indicators;

/// <summary>指标计算用的 K 线序列（double 数组形态）。</summary>
public sealed record KlineSeries(
    string[] Dates,
    double[] Open,
    double[] High,
    double[] Low,
    double[] Close,
    double[] Volume)
{
    public int Length => Dates.Length;

    /// <summary>请求起始日在序列中的下标（lookback 预取部分在此之前，输出时裁掉）。</summary>
    public int IndexOf(string startDate)
    {
        for (var i = 0; i < Dates.Length; i++)
            if (string.CompareOrdinal(Dates[i], startDate) >= 0)
                return i;
        return Dates.Length;
    }
}

/// <summary>
/// 从 REST API 加载 K 线序列。指标计算默认前复权（不复权价在除权日跳空，
/// 会产生虚假信号）；按 lookback 需要自动向前多取并由调用方裁剪。
/// </summary>
public static class KlineLoader
{
    /// <summary>交易日 → 日历日的预取放大系数（A股每年约 242 个交易日）+ 安全余量。</summary>
    private static int LookbackCalendarDays(int lookbackBars) =>
        (int)Math.Ceiling(lookbackBars * 1.7) + 14;

    public static async Task<(KlineSeries? Series, string? Error)> LoadAsync(
        StockDataApiClient api, string code, string startDate, string endDate,
        int lookbackBars, string adjustFlag = "2", CancellationToken ct = default)
    {
        var fetchStart = DateTime.Parse(startDate)
            .AddDays(-LookbackCalendarDays(lookbackBars))
            .ToString("yyyy-MM-dd");
        var json = await api.GetAsync($"/api/v1/stocks/{code}/kline",
            new Dictionary<string, string?>
            {
                ["start_date"] = fetchStart,
                ["end_date"] = endDate,
                ["frequency"] = "d",
                ["adjust_flag"] = adjustFlag,
            }, ct);
        if (json.StartsWith("Error:")) return (null, json);

        try
        {
            using var doc = JsonDocument.Parse(json);
            var n = doc.RootElement.GetArrayLength();
            if (n == 0) return (null, $"Error: {code} {fetchStart}~{endDate} 无K线数据");

            var dates = new string[n];
            var open = new double[n];
            var high = new double[n];
            var low = new double[n];
            var close = new double[n];
            var volume = new double[n];
            var i = 0;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                dates[i] = el.GetProperty("trade_date").GetString()!;
                open[i] = Num(el, "open");
                high[i] = Num(el, "high");
                low[i] = Num(el, "low");
                close[i] = Num(el, "close");
                volume[i] = Num(el, "volume");
                i++;
            }
            return (new KlineSeries(dates, open, high, low, close, volume), null);
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException or FormatException)
        {
            return (null, $"Error: K线数据解析失败: {e.Message}");
        }
    }

    private static double Num(JsonElement el, string prop)
        => el.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetDouble() : double.NaN;
}
