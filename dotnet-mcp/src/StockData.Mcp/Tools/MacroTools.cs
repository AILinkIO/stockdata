using System.ComponentModel;
using ModelContextProtocol.Server;
using StockData.Mcp.Data;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>宏观经济（管线开→dotnet 直读 PG；关→旧 REST 透传，缺省回看 10 年）。</summary>
[McpServerToolType]
public static class MacroTools
{
    // 管线缺省回看窗口（旧 REST 端默认 10 年）
    private static (DateOnly s, DateOnly e) RateRange(string? start, string? end)
    {
        var e = end is not null && DateOnly.TryParse(end, out var pe) ? pe : DateOnly.FromDateTime(DateTime.Today);
        var s = start is not null && DateOnly.TryParse(start, out var ps) ? ps : e.AddYears(-10);
        return (s, e);
    }

    private static async Task<string> Wrap(Task<string> json, int limit) => JsonHelper.Truncate(await json, limit);

    private static Task<string> RateTool(StockDataApiClient api, MacroReadService macro, string kind,
        string path, string? start, string? end, int limit, CancellationToken ct)
    {
        if (macro.Enabled)
        {
            var (s, e) = RateRange(start, end);
            return Wrap(macro.GetRatesJsonAsync(kind, s, e, ct), limit);
        }
        return Wrap(api.GetAsync(path, new() { ["start_date"] = start, ["end_date"] = end }, ct), limit);
    }

    [McpServerTool(Name = "get_deposit_rate_data")]
    [Description("获取基准存款利率（活期/各期限定期）。")]
    public static Task<string> GetDepositRate(StockDataApiClient api, MacroReadService macro,
        string? start_date = null, string? end_date = null, int limit = 250, CancellationToken ct = default)
        => RateTool(api, macro, "deposit_rate", "/api/v1/macro/deposit-rate", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_loan_rate_data")]
    [Description("获取基准贷款利率（各期限）。")]
    public static Task<string> GetLoanRate(StockDataApiClient api, MacroReadService macro,
        string? start_date = null, string? end_date = null, int limit = 250, CancellationToken ct = default)
        => RateTool(api, macro, "loan_rate", "/api/v1/macro/loan-rate", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_required_reserve_ratio_data")]
    [Description("获取存款准备金率（大型/中小型机构调整前后）。")]
    public static Task<string> GetRrr(StockDataApiClient api, MacroReadService macro,
        string? start_date = null, string? end_date = null, int limit = 250, CancellationToken ct = default)
        => RateTool(api, macro, "rrr", "/api/v1/macro/rrr", start_date, end_date, limit, ct);

    [McpServerTool(Name = "get_money_supply_data_month")]
    [Description("获取月度货币供应量（M0/M1/M2 余额与同比环比）。")]
    public static Task<string> GetMoneySupplyMonth(StockDataApiClient api, MacroReadService macro,
        string? start_date = null, string? end_date = null, int limit = 250, CancellationToken ct = default)
    {
        if (macro.Enabled)
        {
            var (s, e) = RateRange(start_date, end_date);
            return Wrap(macro.GetMoneyMonthJsonAsync(s, e, ct), limit);
        }
        return Wrap(api.GetAsync("/api/v1/macro/money-supply/month",
            new() { ["start_date"] = start_date, ["end_date"] = end_date }, ct), limit);
    }

    [McpServerTool(Name = "get_money_supply_data_year")]
    [Description("获取年度货币供应量（M0/M1/M2 年末余额与同比）。")]
    public static Task<string> GetMoneySupplyYear(StockDataApiClient api, MacroReadService macro,
        [Description("起始年份，如 2015")] int? start_year = null,
        [Description("结束年份")] int? end_year = null,
        int limit = 250, CancellationToken ct = default)
    {
        if (macro.Enabled)
        {
            var ey = end_year ?? DateTime.Today.Year;
            var sy = start_year ?? ey - 10;
            return Wrap(macro.GetMoneyYearJsonAsync(sy, ey, ct), limit);
        }
        return Wrap(api.GetAsync("/api/v1/macro/money-supply/year",
            new() { ["start_year"] = start_year?.ToString(), ["end_year"] = end_year?.ToString() }, ct), limit);
    }
}
