using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>代码标准化工具透传。</summary>
[McpServerToolType]
public static class UtilTools
{
    [McpServerTool(Name = "normalize_stock_code")]
    [Description("将任意常见格式的股票代码标准化为 Baostock 格式（如 600000.SH → sh.600000）。")]
    public static Task<string> NormalizeStockCode(StockDataApiClient api,
        [Description("任意格式的股票代码")] string code, CancellationToken ct = default)
        => api.GetAsync("/api/v1/utils/normalize-code", new() { ["code"] = code }, ct);

    [McpServerTool(Name = "normalize_index_code")]
    [Description("将指数代码或英文别名标准化为 Baostock 格式（CSI300/HS300/000300 → sh.000300）。")]
    public static Task<string> NormalizeIndexCode(StockDataApiClient api,
        string code, CancellationToken ct = default)
        => api.GetAsync("/api/v1/utils/normalize-index-code", new() { ["code"] = code }, ct);
}
