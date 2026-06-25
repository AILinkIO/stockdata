using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// baostock 交易日历原始记录 → (日期, 是否交易日)。
/// 移植自 writer.write_trade_calendar：calendar_date 用 _date、is_trading_day 用 _bool01（=='1'）。
/// </summary>
public static class TradeCalendarParser
{
    public static List<(DateOnly Date, bool IsTradingDay)> Parse(FetchPayload payload)
    {
        int di = -1, ti = -1;
        for (var i = 0; i < payload.Fields.Count; i++)
        {
            if (payload.Fields[i] == "calendar_date") di = i;
            else if (payload.Fields[i] == "is_trading_day") ti = i;
        }

        var rows = new List<(DateOnly, bool)>(payload.Rows.Count);
        if (di < 0) return rows;
        foreach (var r in payload.Rows)
        {
            if (di >= r.Count) continue;
            if (KlineParser.Date(r[di]) is not DateOnly d) continue;
            var isTrading = ti >= 0 && ti < r.Count && r[ti] == "1";
            rows.Add((d, isTrading));
        }
        return rows;
    }
}
