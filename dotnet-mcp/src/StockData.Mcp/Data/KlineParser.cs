using System.Globalization;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// baostock 日线原始记录（全字符串）→ <see cref="Kline"/> 解析与列映射。
/// 移植自 <c>fetcher/writer.py</c> 的 _dec/_int/_date/_bool01 与 _K_COL_MAP。
/// 关键：**字符串直接转 decimal（InvariantCulture），绝不经 double/float**；
/// 空串/解析失败 → null（不抛），与 Python 行为一致。
/// </summary>
public static class KlineParser
{
    public static decimal? Dec(string? s)
    {
        if (string.IsNullOrEmpty(s)) return null;
        return decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) ? d : null;
    }

    public static long? Int(string? s)
    {
        if (string.IsNullOrEmpty(s)) return null;
        return long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    public static short? Short(string? s)
    {
        if (string.IsNullOrEmpty(s)) return null;
        return short.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    public static DateOnly? Date(string? s)
    {
        if (string.IsNullOrEmpty(s)) return null;
        return DateOnly.TryParseExact(s, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d)
            ? d : null;
    }

    /// <summary>baostock isST：'1' → true，其余非空 → false，空 → null。</summary>
    public static bool? Bool01(string? s) => string.IsNullOrEmpty(s) ? null : s == "1";

    /// <summary>
    /// 把一行 baostock 日线记录映射为 Kline 实体（code/frequency 由调用方注入）。
    /// 缺列按 null 处理（周/月线无估值/状态列时即缺）。
    /// </summary>
    public static Kline ToKline(IReadOnlyDictionary<string, string?> rec, string code, string frequency) => new()
    {
        Code = code,
        Frequency = frequency,
        TradeDate = Date(Get(rec, "date")) ?? throw new FormatException($"kline 缺少有效 date: {code}"),
        Open = Dec(Get(rec, "open")),
        High = Dec(Get(rec, "high")),
        Low = Dec(Get(rec, "low")),
        Close = Dec(Get(rec, "close")),
        Preclose = Dec(Get(rec, "preclose")),
        Volume = Int(Get(rec, "volume")),
        Amount = Dec(Get(rec, "amount")),
        Turn = Dec(Get(rec, "turn")),
        PctChg = Dec(Get(rec, "pctChg")),
        TradeStatus = Short(Get(rec, "tradestatus")),
        IsSt = Bool01(Get(rec, "isST")),
        PeTtm = Dec(Get(rec, "peTTM")),
        PbMrq = Dec(Get(rec, "pbMRQ")),
        PsTtm = Dec(Get(rec, "psTTM")),
        PcfNcfTtm = Dec(Get(rec, "pcfNcfTTM")),
    };

    private static string? Get(IReadOnlyDictionary<string, string?> rec, string key)
        => rec.TryGetValue(key, out var v) ? v : null;

    /// <summary>payload（fields + rows）→ Kline 列表。最大业务日期由 <see cref="MaxDate"/> 另取。</summary>
    public static List<Kline> ToKlines(FetchPayload payload, string code, string frequency)
    {
        var result = new List<Kline>(payload.Rows.Count);
        foreach (var row in payload.Rows)
        {
            var rec = new Dictionary<string, string?>(payload.Fields.Count);
            for (var i = 0; i < payload.Fields.Count && i < row.Count; i++)
                rec[payload.Fields[i]] = row[i];
            result.Add(ToKline(rec, code, frequency));
        }
        return result;
    }

    /// <summary>payload 中 date 列的最大业务日期（空 payload → null），供 ClaimableLast 使用。</summary>
    public static DateOnly? MaxDate(FetchPayload payload)
    {
        var idx = -1;
        for (var i = 0; i < payload.Fields.Count; i++)
            if (payload.Fields[i] == "date") { idx = i; break; }
        if (idx < 0) return null;

        DateOnly? max = null;
        foreach (var row in payload.Rows)
        {
            if (idx >= row.Count) continue;
            var d = Date(row[idx]);
            if (d is DateOnly dd && (max is null || dd > max)) max = dd;
        }
        return max;
    }
}
