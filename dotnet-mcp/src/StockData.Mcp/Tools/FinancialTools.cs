using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.Data;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>财务报表（管线开→dotnet 直读 PG；关→旧 REST 透传）。</summary>
[McpServerToolType]
public static class FinancialTools
{
    private static Task<string> Quarterly(StockDataApiClient api, FinancialReadService fin, string reportType,
        string code, string year, int quarter, CancellationToken ct)
    {
        if (fin.Enabled && int.TryParse(year, out var y))
            return fin.GetQuarterlyJsonAsync(CodeNormalizer.ToBaostock(code), y, quarter, reportType, ct);
        return api.GetAsync($"/api/v1/stocks/{code}/financials/{reportType}",
            new() { ["year"] = year, ["quarter"] = quarter.ToString() }, ct);
    }

    [McpServerTool(Name = "get_profit_data")]
    [Description("获取季度盈利能力数据（ROE、净利率、毛利率、EPS等）。")]
    public static Task<string> GetProfitData(StockDataApiClient api, FinancialReadService fin, string code,
        [Description("年份，如 2024")] string year, [Description("季度 1-4")] int quarter, CancellationToken ct = default)
        => Quarterly(api, fin, "profit", code, year, quarter, ct);

    [McpServerTool(Name = "get_operation_data")]
    [Description("获取季度营运能力数据（应收/存货/总资产周转率等）。")]
    public static Task<string> GetOperationData(StockDataApiClient api, FinancialReadService fin, string code,
        string year, int quarter, CancellationToken ct = default)
        => Quarterly(api, fin, "operation", code, year, quarter, ct);

    [McpServerTool(Name = "get_growth_data")]
    [Description("获取季度成长能力数据（营收/净利/资产同比增长率等）。")]
    public static Task<string> GetGrowthData(StockDataApiClient api, FinancialReadService fin, string code,
        string year, int quarter, CancellationToken ct = default)
        => Quarterly(api, fin, "growth", code, year, quarter, ct);

    [McpServerTool(Name = "get_balance_data")]
    [Description("获取季度偿债能力数据（流动比率、速动比率、资产负债率等）。")]
    public static Task<string> GetBalanceData(StockDataApiClient api, FinancialReadService fin, string code,
        string year, int quarter, CancellationToken ct = default)
        => Quarterly(api, fin, "balance", code, year, quarter, ct);

    [McpServerTool(Name = "get_cash_flow_data")]
    [Description("获取季度现金流量数据（经营现金流/营收等比率）。")]
    public static Task<string> GetCashFlowData(StockDataApiClient api, FinancialReadService fin, string code,
        string year, int quarter, CancellationToken ct = default)
        => Quarterly(api, fin, "cash_flow", code, year, quarter, ct);

    [McpServerTool(Name = "get_dupont_data")]
    [Description("获取季度杜邦分析数据（ROE 分解）。")]
    public static Task<string> GetDupontData(StockDataApiClient api, FinancialReadService fin, string code,
        string year, int quarter, CancellationToken ct = default)
        => Quarterly(api, fin, "dupont", code, year, quarter, ct);

    private static async Task<string> Perf(StockDataApiClient api, FinancialReadService fin, string reportType,
        string code, string start_date, string end_date, int limit, CancellationToken ct)
    {
        if (fin.Enabled && DateOnly.TryParse(start_date, out var s) && DateOnly.TryParse(end_date, out var e))
            return JsonHelper.Truncate(await fin.GetPerformanceJsonAsync(CodeNormalizer.ToBaostock(code), reportType, s, e, ct), limit);
        return JsonHelper.Truncate(await api.GetAsync($"/api/v1/stocks/{code}/financials/{reportType}",
            new() { ["start_date"] = start_date, ["end_date"] = end_date }, ct), limit);
    }

    [McpServerTool(Name = "get_performance_express_report")]
    [Description("获取业绩快报（按披露日期范围查询）。")]
    public static Task<string> GetPerformanceExpressReport(StockDataApiClient api, FinancialReadService fin,
        string code, string start_date, string end_date, int limit = 250, CancellationToken ct = default)
        => Perf(api, fin, "express", code, start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_forecast_report")]
    [Description("获取业绩预告（按披露日期范围查询）。")]
    public static Task<string> GetForecastReport(StockDataApiClient api, FinancialReadService fin,
        string code, string start_date, string end_date, int limit = 250, CancellationToken ct = default)
        => Perf(api, fin, "forecast", code, start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_fina_indicator")]
    [Description("获取综合财务指标（六类季度财报按报告期合并为一行，字段带类别前缀）。")]
    public static async Task<string> GetFinaIndicator(StockDataApiClient api, FinancialReadService fin,
        string code, string start_date, string end_date, int limit = 250, CancellationToken ct = default)
    {
        if (fin.Enabled && DateOnly.TryParse(start_date, out var s) && DateOnly.TryParse(end_date, out var e))
            return JsonHelper.Truncate(await fin.GetIndicatorJsonAsync(CodeNormalizer.ToBaostock(code), s, e, ct), limit);
        return JsonHelper.Truncate(await api.GetAsync($"/api/v1/stocks/{code}/financials/indicator",
            new() { ["start_date"] = start_date, ["end_date"] = end_date }, ct), limit);
    }
}
