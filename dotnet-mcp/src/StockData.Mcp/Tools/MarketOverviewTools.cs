using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>市场概览透传：交易日历、股票列表及其衍生（搜索/停牌）。</summary>
[McpServerToolType]
public static class MarketOverviewTools
{
    [McpServerTool(Name = "get_trade_dates")]
    [Description("获取交易日历（每天标注是否交易日）。缺省返回最近 90 天。")]
    public static async Task<string> GetTradeDates(StockDataApiClient api,
        string? start_date = null, string? end_date = null, int limit = 250,
        CancellationToken ct = default)
    {
        var end = end_date ?? DateTime.Today.ToString("yyyy-MM-dd");
        var start = start_date
            ?? DateTime.Parse(end).AddDays(-90).ToString("yyyy-MM-dd");
        return JsonHelper.Truncate(await api.GetAsync("/api/v1/market/trade-calendar",
            new() { ["start_date"] = start, ["end_date"] = end }, ct), limit);
    }

    [McpServerTool(Name = "get_all_stock")]
    [Description("获取全部股票（含指数）列表及交易状态。缺省最新交易日快照。")]
    public static async Task<string> GetAllStock(StockDataApiClient api,
        string? date = null, int limit = 250, CancellationToken ct = default)
        => JsonHelper.Truncate(await api.GetAsync("/api/v1/market/stocks",
            new() { ["snap_date"] = date }, ct), limit);

    [McpServerTool(Name = "search_stocks")]
    [Description("按代码或名称关键字搜索股票。")]
    public static async Task<string> SearchStocks(StockDataApiClient api,
        [Description("关键字（代码或名称子串）")] string keyword,
        string? date = null, int limit = 50, CancellationToken ct = default)
    {
        var json = await api.GetAsync("/api/v1/market/stocks",
            new() { ["snap_date"] = date }, ct);
        var kw = keyword.Trim();
        return JsonHelper.FilterArray(json,
            el => (el.Str("code") ?? "").Contains(kw, StringComparison.OrdinalIgnoreCase)
                  || (el.Str("code_name") ?? "").Contains(kw, StringComparison.OrdinalIgnoreCase),
            limit, $"Error: 未找到匹配 '{keyword}' 的股票");
    }

    [McpServerTool(Name = "get_suspensions")]
    [Description("获取停牌股票列表。")]
    public static async Task<string> GetSuspensions(StockDataApiClient api,
        string? date = null, int limit = 250, CancellationToken ct = default)
    {
        var json = await api.GetAsync("/api/v1/market/stocks",
            new() { ["snap_date"] = date }, ct);
        return JsonHelper.FilterArray(json,
            el => el.TryGetProperty("trade_status", out var v)
                  && v.ValueKind == System.Text.Json.JsonValueKind.False,
            limit, "[]");
    }
}
