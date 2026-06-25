using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.Data;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>股票行情透传（/api/v1/stocks/...）。工具名与旧 Python MCP 兼容。</summary>
[McpServerToolType]
public static class MarketDataTools
{
    [McpServerTool(Name = "get_historical_k_data")]
    [Description("获取股票历史K线数据。frequency: d日/w周/m月/5/15/30/60分钟；adjust_flag: 1后复权/2前复权/3不复权。代码支持 sh.600000/600000/600000.SH 等格式。")]
    public static async Task<string> GetHistoricalKData(
        StockDataApiClient api,
        KlineReadService pipeline,
        [Description("股票代码，如 sh.600000")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        [Description("频率：d/w/m/5/15/30/60")] string frequency = "d",
        [Description("复权：1后复权/2前复权/3不复权")] string adjust_flag = "3",
        [Description("最大返回行数")] int limit = 250,
        CancellationToken ct = default)
    {
        var isMinute = frequency is "5" or "15" or "30" or "60";

        // 管线开启 + d/w/m 不复权：走 dotnet（EnsureRange + 直读 PG）。
        // 分钟线 / 复权（adjust_flag 1/2）尚未迁移，仍回退旧 Python REST。
        if (pipeline.Enabled && !isMinute && adjust_flag == "3"
            && DateOnly.TryParse(start_date, out var s) && DateOnly.TryParse(end_date, out var e))
        {
            var norm = CodeNormalizer.ToBaostock(code);
            return JsonHelper.Truncate(await pipeline.GetHistoricalJsonAsync(norm, frequency, s, e, ct), limit);
        }

        var path = isMinute
            ? $"/api/v1/stocks/{code}/kline-minute"
            : $"/api/v1/stocks/{code}/kline";
        var query = new Dictionary<string, string?>
        {
            ["start_date"] = start_date,
            ["end_date"] = end_date,
            ["frequency"] = frequency,
        };
        if (!isMinute) query["adjust_flag"] = adjust_flag;
        return JsonHelper.Truncate(await api.GetAsync(path, query, ct), limit);
    }

    [McpServerTool(Name = "get_stock_basic_info")]
    [Description("获取股票基本信息（名称、上市/退市日期、类型、状态）。")]
    public static Task<string> GetStockBasicInfo(StockDataApiClient api,
        [Description("股票代码")] string code, CancellationToken ct = default)
        => api.GetAsync($"/api/v1/stocks/{code}/basic", ct: ct);

    [McpServerTool(Name = "get_dividend_data")]
    [Description("获取分红送转数据。year_type: report预案公告年份/operate除权除息年份。")]
    public static async Task<string> GetDividendData(StockDataApiClient api,
        [Description("股票代码")] string code,
        [Description("年份，如 2023")] string year,
        [Description("report/operate")] string year_type = "report",
        int limit = 250, CancellationToken ct = default)
        => JsonHelper.Truncate(await api.GetAsync($"/api/v1/stocks/{code}/dividends",
            new() { ["year"] = year, ["year_type"] = year_type }, ct), limit);

    [McpServerTool(Name = "get_adjust_factor_data")]
    [Description("获取复权因子数据（每个除权除息事件一行，含前/后复权因子）。")]
    public static async Task<string> GetAdjustFactorData(StockDataApiClient api,
        [Description("股票代码")] string code,
        [Description("起始日期 YYYY-MM-DD")] string start_date,
        [Description("结束日期 YYYY-MM-DD")] string end_date,
        int limit = 250, CancellationToken ct = default)
        => JsonHelper.Truncate(await api.GetAsync($"/api/v1/stocks/{code}/adjust-factors",
            new() { ["start_date"] = start_date, ["end_date"] = end_date }, ct), limit);

    [McpServerTool(Name = "get_stock_analysis")]
    [Description("生成个股分析报告（Markdown）。analysis_type: fundamental基本面/technical技术面/comprehensive综合。")]
    public static Task<string> GetStockAnalysis(StockDataApiClient api,
        [Description("股票代码")] string code,
        [Description("fundamental/technical/comprehensive")] string analysis_type = "comprehensive",
        CancellationToken ct = default)
        => api.GetAsync($"/api/v1/stocks/{code}/analysis",
            new() { ["analysis_type"] = analysis_type }, ct);
}
