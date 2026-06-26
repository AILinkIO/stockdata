using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data;

/// <summary>复权因子原始数据读取（get_adjust_factor_data）：EnsureFull → 直读区间。</summary>
public sealed class AdjustFactorReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public async Task<string> GetJsonAsync(string code, DateOnly start, DateOnly end, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        else await sp.GetRequiredService<AdjustFactorService>().EnsureFullAsync(code, end, now, ct);

        const string sql =
            "SELECT COALESCE(json_agg(json_build_object('code',t.code,'divid_operate_date',t.divid_operate_date," +
            "'fore_adjust_factor',t.fore_adjust_factor,'back_adjust_factor',t.back_adjust_factor," +
            "'adjust_factor',t.adjust_factor) ORDER BY t.divid_operate_date),'[]')::text AS \"Value\" " +
            "FROM adjust_factor t WHERE t.code = {0} AND t.divid_operate_date >= {1} AND t.divid_operate_date <= {2}";
        return await db.Database.SqlQueryRaw<string>(sql, code, start, end).FirstAsync(ct);
    }
}

/// <summary>
/// 个股分析报告（Markdown，移植 analysis.build_stock_analysis_report）：聚合基本信息/行业/财报/K线。
/// 复用已迁的各读服务（均为单例，按 JSON 解析取值）。
/// </summary>
public sealed class StockAnalysisService(
    StockBasicReadService basic, SnapshotReadService snap, FinancialReadService fin,
    KlineReadService kline, TimeProvider time, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    public async Task<string> BuildAsync(string code, string analysisType, CancellationToken ct = default)
    {
        if (analysisType is not ("fundamental" or "technical" or "comprehensive"))
            return "Error: analysis_type 必须为 fundamental / technical / comprehensive";

        var c = CodeNormalizer.ToBaostock(code);
        var today = Coverage.Today(time.GetUtcNow());
        var sb = new StringBuilder();

        var basicJson = await basic.GetJsonAsync(c, ct);
        string? name = null, ipo = null;
        if (TryObj(basicJson, out var b))
        {
            name = Str(b, "code_name");
            ipo = Str(b, "ipo_date");
        }
        var stockName = name ?? code;

        sb.Append($"# {stockName} 数据分析报告\n\n");
        sb.Append("## 免责声明\n本报告基于公开数据生成，仅供参考，不构成投资建议。投资决策需基于个人风险承受能力和研究。\n\n");

        if (name is not null)
        {
            var indJson = await snap.IndustryJsonAsync(c, null, ct);
            var industry = FirstStr(indJson, "industry") ?? "未知";
            sb.Append("## 公司基本信息\n");
            sb.Append($"- 股票代码: {code}\n- 股票名称: {stockName}\n- 所属行业: {industry}\n- 上市日期: {ipo ?? "未知"}\n\n");
        }

        if (analysisType is "fundamental" or "comprehensive")
        {
            var (year, quarter) = (today.Year, (today.Month - 1) / 3 + 1);
            var rows = await fin.GetQuarterlyJsonAsync(c, year, quarter, null, ct);
            if (!HasItems(rows))
            {
                (year, quarter) = quarter == 1 ? (year - 1, 4) : (year, quarter - 1);
                rows = await fin.GetQuarterlyJsonAsync(c, year, quarter, null, ct);
            }
            if (HasItems(rows))
            {
                sb.Append($"## 基本面指标分析 ({year}年第{quarter}季度)\n\n### 盈利能力指标\n");
                Line(sb, rows, "profit", "roeAvg", "ROE(净资产收益率)", "%");
                Line(sb, rows, "profit", "npMargin", "销售净利率", "%");
                sb.Append("\n### 成长能力指标\n");
                Line(sb, rows, "growth", "YOYEquity", "净资产同比增长", "%");
                Line(sb, rows, "growth", "YOYAsset", "总资产同比增长", "%");
                Line(sb, rows, "growth", "YOYNI", "净利润同比增长", "%");
                sb.Append("\n### 偿债能力指标\n");
                Line(sb, rows, "balance", "currentRatio", "流动比率", "");
                Line(sb, rows, "balance", "assetLiabRatio", "资产负债率", "%");
            }
        }

        if (analysisType is "technical" or "comprehensive")
        {
            var klineJson = await kline.GetHistoricalJsonAsync(c, "d", today.AddDays(-180), today, "2", ct);
            var closes = Closes(klineJson);
            if (closes.Count > 0)
            {
                sb.Append("\n## 技术面简析（近180日）\n");
                var change = closes[0] != 0 ? (closes[^1] - closes[0]) / closes[0] * 100 : 0;
                sb.Append($"- 区间涨跌幅: {change.ToString("F2", CultureInfo.InvariantCulture)}%\n");
                if (closes.Count >= 20)
                {
                    var ma20 = closes.Skip(closes.Count - 20).Sum() / 20;
                    sb.Append($"- 20日均线: {ma20.ToString("F2", CultureInfo.InvariantCulture)}\n");
                }
            }
        }

        return sb.ToString();
    }

    // ── JSON 取值助手 ──
    private static bool TryObj(string json, out JsonElement obj)
    {
        obj = default;
        try { var d = JsonDocument.Parse(json); if (d.RootElement.ValueKind == JsonValueKind.Object) { obj = d.RootElement.Clone(); return true; } }
        catch (JsonException) { }
        return false;
    }

    private static string? Str(JsonElement e, string p)
        => e.TryGetProperty(p, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static bool HasItems(string json)
    {
        try { using var d = JsonDocument.Parse(json); return d.RootElement.ValueKind == JsonValueKind.Array && d.RootElement.GetArrayLength() > 0; }
        catch (JsonException) { return false; }
    }

    private static string? FirstStr(string json, string p)
    {
        try
        {
            using var d = JsonDocument.Parse(json);
            if (d.RootElement.ValueKind == JsonValueKind.Array && d.RootElement.GetArrayLength() > 0)
                return Str(d.RootElement[0], p);
        }
        catch (JsonException) { }
        return null;
    }

    // 财报数组中取 report_type 行的 metrics[key]
    private static void Line(StringBuilder sb, string rowsJson, string reportType, string key, string label, string unit)
    {
        try
        {
            using var d = JsonDocument.Parse(rowsJson);
            foreach (var r in d.RootElement.EnumerateArray())
                if (Str(r, "report_type") == reportType && r.TryGetProperty("metrics", out var m)
                    && m.TryGetProperty(key, out var v) && v.ValueKind != JsonValueKind.Null)
                {
                    var val = v.ValueKind == JsonValueKind.String ? v.GetString() : v.ToString();
                    sb.Append($"- {label}: {val}{unit}\n");
                    return;
                }
        }
        catch (JsonException) { }
    }

    private static List<double> Closes(string klineJson)
    {
        var closes = new List<double>();
        try
        {
            using var d = JsonDocument.Parse(klineJson);
            if (d.RootElement.ValueKind == JsonValueKind.Array)
                foreach (var bar in d.RootElement.EnumerateArray())
                    if (bar.TryGetProperty("close", out var v) && v.ValueKind == JsonValueKind.Number)
                        closes.Add(v.GetDouble());
        }
        catch (JsonException) { }
        return closes;
    }
}
