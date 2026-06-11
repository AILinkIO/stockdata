using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>宏观经济透传（/api/v1/macro/...）。日期缺省由 REST 端处理（回看 10 年）。</summary>
[McpServerToolType]
public static class MacroTools
{
    private static async Task<string> Range(StockDataApiClient api, string path,
        string? start, string? end, int limit, CancellationToken ct)
        => JsonHelper.Truncate(await api.GetAsync(path,
            new() { ["start_date"] = start, ["end_date"] = end }, ct), limit);

    [McpServerTool(Name = "get_deposit_rate_data")]
    [Description("获取基准存款利率（活期/各期限定期）。")]
    public static Task<string> GetDepositRate(StockDataApiClient api,
        string? start_date = null, string? end_date = null, int limit = 250,
        CancellationToken ct = default)
        => Range(api, "/api/v1/macro/deposit-rate", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_loan_rate_data")]
    [Description("获取基准贷款利率（各期限）。")]
    public static Task<string> GetLoanRate(StockDataApiClient api,
        string? start_date = null, string? end_date = null, int limit = 250,
        CancellationToken ct = default)
        => Range(api, "/api/v1/macro/loan-rate", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_required_reserve_ratio_data")]
    [Description("获取存款准备金率（大型/中小型机构调整前后）。")]
    public static Task<string> GetRrr(StockDataApiClient api,
        string? start_date = null, string? end_date = null, int limit = 250,
        CancellationToken ct = default)
        => Range(api, "/api/v1/macro/rrr", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_money_supply_data_month")]
    [Description("获取月度货币供应量（M0/M1/M2 余额与同比环比）。")]
    public static Task<string> GetMoneySupplyMonth(StockDataApiClient api,
        string? start_date = null, string? end_date = null, int limit = 250,
        CancellationToken ct = default)
        => Range(api, "/api/v1/macro/money-supply/month", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_money_supply_data_year")]
    [Description("获取年度货币供应量（M0/M1/M2 年末余额与同比）。")]
    public static async Task<string> GetMoneySupplyYear(StockDataApiClient api,
        [Description("起始年份，如 2015")] int? start_year = null,
        [Description("结束年份")] int? end_year = null,
        int limit = 250, CancellationToken ct = default)
        => JsonHelper.Truncate(await api.GetAsync("/api/v1/macro/money-supply/year",
            new()
            {
                ["start_year"] = start_year?.ToString(),
                ["end_year"] = end_year?.ToString(),
            }, ct), limit);
}
