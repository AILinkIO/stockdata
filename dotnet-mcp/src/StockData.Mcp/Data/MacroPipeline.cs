using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>宏观一列：baostock 字段 → 列名 + 转换（d=date, n=decimal, i=short）。</summary>
public readonly record struct MacroColumn(string Field, string Col, char Conv);

/// <summary>宏观一类的表/主键/列映射（移植 writer._MACRO_SPECS）。</summary>
public sealed record MacroSpec(string Table, string[] Pk, MacroColumn[] Cols, string OrderBy);

public static class MacroSpecs
{
    public static readonly IReadOnlyDictionary<string, MacroSpec> All = new Dictionary<string, MacroSpec>
    {
        ["deposit_rate"] = new("deposit_rate", new[] { "pub_date" }, new[]
        {
            new MacroColumn("pubDate", "pub_date", 'd'),
            new("demandDepositRate", "demand_deposit_rate", 'n'),
            new("fixedDepositRate3Month", "fixed_deposit_rate_3month", 'n'),
            new("fixedDepositRate6Month", "fixed_deposit_rate_6month", 'n'),
            new("fixedDepositRate1Year", "fixed_deposit_rate_1year", 'n'),
            new("fixedDepositRate2Year", "fixed_deposit_rate_2year", 'n'),
            new("fixedDepositRate3Year", "fixed_deposit_rate_3year", 'n'),
            new("fixedDepositRate5Year", "fixed_deposit_rate_5year", 'n'),
            new("installmentFixedDepositRate1Year", "installment_fixed_deposit_rate_1year", 'n'),
            new("installmentFixedDepositRate3Year", "installment_fixed_deposit_rate_3year", 'n'),
            new("installmentFixedDepositRate5Year", "installment_fixed_deposit_rate_5year", 'n'),
        }, "pub_date"),
        ["loan_rate"] = new("loan_rate", new[] { "pub_date" }, new[]
        {
            new MacroColumn("pubDate", "pub_date", 'd'),
            new("loanRate6Month", "loan_rate_6month", 'n'),
            new("loanRate6MonthTo1Year", "loan_rate_6month_to_1year", 'n'),
            new("loanRate1YearTo3Year", "loan_rate_1year_to_3year", 'n'),
            new("loanRate3YearTo5Year", "loan_rate_3year_to_5year", 'n'),
            new("loanRateAbove5Year", "loan_rate_above_5year", 'n'),
            new("mortgateRateBelow5Year", "mortgage_rate_below_5year", 'n'),
            new("mortgateRateAbove5Year", "mortgage_rate_above_5year", 'n'),
        }, "pub_date"),
        ["rrr"] = new("required_reserve_ratio", new[] { "pub_date", "effective_date" }, new[]
        {
            new MacroColumn("pubDate", "pub_date", 'd'),
            new("effectiveDate", "effective_date", 'd'),
            new("bigInstitutionsRatioPre", "big_institutions_ratio_pre", 'n'),
            new("bigInstitutionsRatioAfter", "big_institutions_ratio_after", 'n'),
            new("mediumInstitutionsRatioPre", "medium_institutions_ratio_pre", 'n'),
            new("mediumInstitutionsRatioAfter", "medium_institutions_ratio_after", 'n'),
        }, "pub_date"),
        ["money_supply_month"] = new("money_supply_month", new[] { "stat_year", "stat_month" }, new[]
        {
            new MacroColumn("statYear", "stat_year", 'i'),
            new("statMonth", "stat_month", 'i'),
            new("m0Month", "m0_month", 'n'), new("m0YOY", "m0_yoy", 'n'), new("m0ChainRelative", "m0_chain_relative", 'n'),
            new("m1Month", "m1_month", 'n'), new("m1YOY", "m1_yoy", 'n'), new("m1ChainRelative", "m1_chain_relative", 'n'),
            new("m2Month", "m2_month", 'n'), new("m2YOY", "m2_yoy", 'n'), new("m2ChainRelative", "m2_chain_relative", 'n'),
        }, "stat_year,stat_month"),
        ["money_supply_year"] = new("money_supply_year", new[] { "stat_year" }, new[]
        {
            new MacroColumn("statYear", "stat_year", 'i'),
            new("m0Year", "m0_year", 'n'), new("m0YearYOY", "m0_year_yoy", 'n'),
            new("m1Year", "m1_year", 'n'), new("m1YearYOY", "m1_year_yoy", 'n'),
            new("m2Year", "m2_year", 'n'), new("m2YearYOY", "m2_year_yoy", 'n'),
        }, "stat_year"),
    };
}

/// <summary>spec 驱动解析：payload → 与 spec.Cols 等长的行。</summary>
public static class MacroParser
{
    public static List<object?[]> Parse(MacroSpec spec, FetchPayload p)
    {
        var get = SnapshotSql.Accessor(p);
        var rows = new List<object?[]>();
        foreach (var r in p.Rows)
        {
            var row = new object?[spec.Cols.Length];
            var keyMissing = false;
            for (var i = 0; i < spec.Cols.Length; i++)
            {
                var c = spec.Cols[i];
                var s = get(r, c.Field);
                row[i] = c.Conv switch { 'd' => KlineParser.Date(s), 'i' => KlineParser.Short(s), _ => KlineParser.Dec(s) };
                if (spec.Pk.Contains(c.Col) && row[i] is null) keyMissing = true;   // 主键缺失则丢行
            }
            if (!keyMissing) rows.Add(row);
        }
        return rows;
    }
}

public interface IMacroWriter
{
    Task<int> PersistAsync(string kind, DateOnly first, DateOnly last, FetchPayload payload, CancellationToken ct = default);
}

public sealed class MacroWriter(StockDataDbContext db) : IMacroWriter
{
    public async Task<int> PersistAsync(string kind, DateOnly first, DateOnly last, FetchPayload payload, CancellationToken ct = default)
    {
        var spec = MacroSpecs.All[kind];
        var rows = MacroParser.Parse(spec, payload);

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var n = rows.Count == 0 ? 0
            : await SnapshotSql.UpsertAsync(db, spec.Table, spec.Cols.Select(c => c.Col).ToArray(), spec.Pk, rows, ct);

        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES ('',@dt,@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date = LEAST(data_watermark.first_date, EXCLUDED.first_date),
                last_date  = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("dt", kind), new NpgsqlParameter("first", first), new NpgsqlParameter("last", last));

        await tx.CommitAsync(ct);
        return n;
    }
}

/// <summary>
/// 宏观编排（范围类）：利率类按 ISO 日期、货币供应按 YYYY-MM/YYYY 抓取，水位折成 date。
/// 移植 readthrough 的 _range_task(macro) + fetch_macro 的 _macro_dates 水位口径。code=""，不切片。
/// </summary>
public sealed class MacroService(IWatermarkStore watermarks, IFetchClient fetch, IMacroWriter writer)
{
    public async Task EnsureRangeAsync(string kind, DateOnly start, DateOnly end, DateTimeOffset now, CancellationToken ct = default)
    {
        var wm = await watermarks.GetAsync("", kind, ct);
        var decision = Coverage.CheckRange(wm?.ToWatermark(), kind, start, end, now);
        if (decision.Fresh) return;

        var today = Coverage.Today(now);
        foreach (var (fs, fe) in decision.FetchRanges)
        {
            var (startRaw, endRaw, wmFirst, wmLast) = kind switch
            {
                "money_supply_month" => (fs.ToString("yyyy-MM"), fe.ToString("yyyy-MM"),
                    new DateOnly(fs.Year, fs.Month, 1), new DateOnly(fe.Year, fe.Month, 1)),
                "money_supply_year" => (fs.Year.ToString(), fe.Year.ToString(),
                    new DateOnly(fs.Year, 1, 1), new DateOnly(fe.Year, 1, 1)),
                _ => (fs.ToString("yyyy-MM-dd"), fe.ToString("yyyy-MM-dd"), fs, fe),
            };
            var payload = await fetch.FetchAsync(
                new FetchRequest("fetch_macro", Kind: kind, StartRaw: startRaw, EndRaw: endRaw), ct);
            await writer.PersistAsync(kind, wmFirst, wmLast < today ? wmLast : today, payload, ct);
        }
    }
}
