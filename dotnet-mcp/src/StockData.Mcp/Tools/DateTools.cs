using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>日期工具（透传 /api/v1/dates/...）。</summary>
[McpServerToolType]
public static class DateTools
{
    [McpServerTool(Name = "get_latest_trading_date")]
    [Description("获取最近的交易日（今天若是交易日则返回今天），格式 YYYY-MM-DD。")]
    public static Task<string> GetLatestTradingDate(StockDataApiClient api, CancellationToken ct)
        => api.GetAsync("/api/v1/dates/latest-trading-day", ct: ct);
}
