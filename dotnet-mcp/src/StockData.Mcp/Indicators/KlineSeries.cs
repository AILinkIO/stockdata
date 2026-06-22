using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Indicators;

/// <summary>
/// TALib 计算所需的 OHLCV 数组，与 REST API 返回的 JSON 对齐。
/// </summary>
public sealed class KlineSeries
{
    public string[] Dates { get; }
    public double[] Open { get; }
    public double[] High { get; }
    public double[] Low { get; }
    public double[] Close { get; }
    public double[] Volume { get; }
    public int Length => Dates.Length;

    public KlineSeries(string[] dates, double[] open, double[] high,
        double[] low, double[] close, double[] volume)
    {
        Dates = dates; Open = open; High = high;
        Low = low; Close = close; Volume = volume;
    }

    /// <summary>返回第一个 Dates[i] >= date 的索引，找不到则返回 Length。</summary>
    public int IndexOf(string date)
    {
        for (var i = 0; i < Dates.Length; i++)
            if (string.Compare(Dates[i], date, StringComparison.Ordinal) >= 0) return i;
        return Length;
    }
}

/// <summary>
/// 带内存缓存的 K 线加载器。
///
/// 缓存分层：
///   - 历史数据（end_date &lt; 今日）：TTL 24h，数据不再变化
///   - 含当日数据：TTL 5min，盘中可能刷新
///
/// 缓存键覆盖实际请求的扩展区间（含 lookback 预热 bar），
/// 相同参数的 RSI/OBV/CCI 并发请求命中同一条目。
/// </summary>
public static class KlineLoader
{
    // 把"预热 bar 数"换算成需向前扩展的自然日数：每根 K 线约跨多少自然日（含容错余量）。
    // 日线 1 交易日≈1.5 自然日；周线≈1 周；月线≈1 月。周/月线若仍按日线系数换算，预热区间
    // 会严重不足，导致起始段均线全为 null。
    private static double CalendarDaysPerBar(string frequency) => frequency switch
    {
        "w" => 8.0,
        "m" => 33.0,
        _   => 1.6,   // "d"
    };

    public static async Task<(KlineSeries? Series, string? Error)> LoadAsync(
        StockDataApiClient api, IMemoryCache cache,
        string code, string startDate, string endDate,
        int extraBars, string adjustFlag, string frequency, CancellationToken ct)
    {
        if (frequency is not ("d" or "w" or "m"))
            return (null, $"Error: frequency 仅支持 d(日)/w(周)/m(月)，当前为 {frequency}");

        var extStart = DateTime.Parse(startDate)
            .AddDays(-(int)(extraBars * CalendarDaysPerBar(frequency)))
            .ToString("yyyy-MM-dd");

        // frequency 必须进缓存键，否则日/周/月线会相互串味
        var key = $"kline:{code}:{frequency}:{extStart}:{endDate}:{adjustFlag}";
        if (cache.TryGetValue(key, out KlineSeries? hit)) return (hit, null);

        var json = await api.GetAsync($"/api/v1/stocks/{code}/kline",
            new() { ["start_date"] = extStart, ["end_date"] = endDate,
                    ["frequency"] = frequency, ["adjust_flag"] = adjustFlag }, ct);

        if (json.StartsWith("Error:")) return (null, json);

        var (series, error) = Parse(code, json, extStart, endDate);
        if (series is not null && !ct.IsCancellationRequested)
        {
            var ttl = DateTime.Parse(endDate) < DateTime.Today
                ? TimeSpan.FromHours(24)
                : TimeSpan.FromMinutes(5);
            cache.Set(key, series, ttl);
        }
        return (series, error);
    }

    private static (KlineSeries? Series, string? Error) Parse(
        string code, string json, string extStart, string endDate)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                return (null, $"Error: {code} K线响应格式异常");

            var arr = doc.RootElement.EnumerateArray().ToArray();
            if (arr.Length == 0)
                return (null, $"Error: {code} 在 {extStart}~{endDate} 无K线数据");

            var n = arr.Length;
            var dates  = new string[n];
            var open   = new double[n];
            var high   = new double[n];
            var low    = new double[n];
            var close  = new double[n];
            var volume = new double[n];

            for (var i = 0; i < n; i++)
            {
                var el = arr[i];
                dates[i]  = el.GetProperty("trade_date").GetString()!;
                open[i]   = GetDouble(el, "open",   i > 0 ? open[i - 1]  : 0.0);
                high[i]   = GetDouble(el, "high",   i > 0 ? high[i - 1]  : 0.0);
                low[i]    = GetDouble(el, "low",    i > 0 ? low[i - 1]   : 0.0);
                close[i]  = GetDouble(el, "close",  i > 0 ? close[i - 1] : 0.0);
                volume[i] = GetLong(el, "volume");
            }
            return (new KlineSeries(dates, open, high, low, close, volume), null);
        }
        catch (Exception e)
        {
            return (null, $"Error: 解析K线数据失败: {e.Message}");
        }
    }

    private static double GetDouble(JsonElement el, string prop, double fallback)
    {
        if (!el.TryGetProperty(prop, out var v) || v.ValueKind == JsonValueKind.Null) return fallback;
        return v.GetDouble();
    }

    private static double GetLong(JsonElement el, string prop)
    {
        if (!el.TryGetProperty(prop, out var v) || v.ValueKind == JsonValueKind.Null) return 0.0;
        return v.ValueKind == JsonValueKind.Number ? (double)v.GetInt64() : 0.0;
    }
}
