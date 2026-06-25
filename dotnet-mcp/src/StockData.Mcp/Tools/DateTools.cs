using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using StockData.Mcp.Data;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>交易日派生工具（管线开→dotnet 基于 trade_calendar 计算；关→旧 REST /api/v1/dates/*）。</summary>
[McpServerToolType]
public static class DateTools
{
    [McpServerTool(Name = "get_latest_trading_date")]
    [Description("获取最近的交易日（今天若是交易日则返回今天），格式 YYYY-MM-DD。")]
    public static Task<string> GetLatestTradingDate(StockDataApiClient api, TradingDaysReadService dates,
        CancellationToken ct = default)
        => dates.Enabled ? dates.LatestTradingDateAsync(ct) : api.GetAsync("/api/v1/dates/latest-trading-day", ct: ct);

    [McpServerTool(Name = "is_trading_day")]
    [Description("判断指定日期（YYYY-MM-DD）是否为 A 股交易日。")]
    public static Task<string> IsTradingDay(StockDataApiClient api, TradingDaysReadService dates,
        [Description("日期 YYYY-MM-DD")] string date, CancellationToken ct = default)
        => dates.Enabled && DateOnly.TryParse(date, out var d)
            ? dates.IsTradingDayAsync(d, ct)
            : api.GetAsync("/api/v1/dates/is-trading-day", new() { ["date"] = date }, ct);

    [McpServerTool(Name = "previous_trading_day")]
    [Description("获取指定日期之前最近的交易日。")]
    public static Task<string> PreviousTradingDay(StockDataApiClient api, TradingDaysReadService dates,
        string date, CancellationToken ct = default)
        => dates.Enabled && DateOnly.TryParse(date, out var d)
            ? dates.PreviousTradingDayAsync(d, ct)
            : api.GetAsync("/api/v1/dates/previous-trading-day", new() { ["date"] = date }, ct);

    [McpServerTool(Name = "next_trading_day")]
    [Description("获取指定日期之后最近的交易日。")]
    public static Task<string> NextTradingDay(StockDataApiClient api, TradingDaysReadService dates,
        string date, CancellationToken ct = default)
        => dates.Enabled && DateOnly.TryParse(date, out var d)
            ? dates.NextTradingDayAsync(d, ct)
            : api.GetAsync("/api/v1/dates/next-trading-day", new() { ["date"] = date }, ct);

    [McpServerTool(Name = "get_last_n_trading_days")]
    [Description("获取最近 N 个交易日列表（升序）。")]
    public static Task<string> GetLastNTradingDays(StockDataApiClient api, TradingDaysReadService dates,
        [Description("天数，1-250")] int days = 5, CancellationToken ct = default)
        => dates.Enabled
            ? dates.LastNTradingDaysAsync(days, ct)
            : api.GetAsync("/api/v1/dates/last-trading-days", new() { ["days"] = days.ToString() }, ct);

    [McpServerTool(Name = "get_recent_trading_range")]
    [Description("获取最近 N 个交易日的起止日期（适合作为 K 线查询范围）。")]
    public static async Task<string> GetRecentTradingRange(StockDataApiClient api, TradingDaysReadService dates,
        int days = 5, CancellationToken ct = default)
    {
        if (dates.Enabled) return await dates.RecentRangeAsync(days, ct);

        var json = await api.GetAsync("/api/v1/dates/last-trading-days", new() { ["days"] = days.ToString() }, ct);
        if (json.StartsWith("Error:")) return json;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var arr = doc.RootElement.GetProperty("dates");
            if (arr.GetArrayLength() == 0) return "Error: 交易日历数据缺失";
            return $"{{\"start_date\": \"{arr[0].GetString()}\", \"end_date\": \"{arr[arr.GetArrayLength() - 1].GetString()}\"}}";
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException)
        {
            return json;
        }
    }
}
