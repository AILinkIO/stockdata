using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// 分红送转解析（移植 write_dividend）：plan_announce_date 必填（缺则跳过），关键日期/比例落列，
/// 其余 baostock 字段进 detail(JSONB)。code/year/year_type 由调用方注入。
/// </summary>
public static class DividendParser
{
    private static readonly Dictionary<string, string> Typed = new()
    {
        ["dividRegistDate"] = "regist_date", ["dividOperateDate"] = "operate_date", ["dividPayDate"] = "pay_date",
        ["dividCashPsBeforeTax"] = "cash_ps_before_tax", ["dividCashPsAfterTax"] = "cash_ps_after_tax",
        ["dividStocksPs"] = "stocks_ps", ["dividReserveToStockPs"] = "reserve_to_stock_ps",
    };

    private static readonly JsonSerializerOptions DetailJson = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    public static readonly string[] Columns =
    {
        "code", "plan_announce_date", "year_type", "year", "regist_date", "operate_date", "pay_date",
        "cash_ps_before_tax", "cash_ps_after_tax", "stocks_ps", "reserve_to_stock_ps", "detail",
    };

    /// <summary>解析为与 <see cref="Columns"/> 等长的行（detail 为 jsonb 文本或 null）。</summary>
    public static List<object?[]> Parse(FetchPayload p, string code, int year, string yearType)
    {
        var get = SnapshotSql.Accessor(p);
        var rows = new List<object?[]>();
        foreach (var r in p.Rows)
        {
            if (KlineParser.Date(get(r, "dividPlanAnnounceDate")) is not DateOnly plan) continue;

            var detail = new Dictionary<string, string?>();
            for (var i = 0; i < p.Fields.Count; i++)
            {
                var f = p.Fields[i];
                if (f is "code" or "dividPlanAnnounceDate" || Typed.ContainsKey(f)) continue;
                detail[f] = i < r.Count ? r[i] : null;
            }
            var detailJson = detail.Count == 0 ? null : JsonSerializer.Serialize(detail, DetailJson);

            rows.Add(new object?[]
            {
                code, plan, yearType, (short)year,
                KlineParser.Date(get(r, "dividRegistDate")), KlineParser.Date(get(r, "dividOperateDate")),
                KlineParser.Date(get(r, "dividPayDate")),
                KlineParser.Dec(get(r, "dividCashPsBeforeTax")), KlineParser.Dec(get(r, "dividCashPsAfterTax")),
                KlineParser.Dec(get(r, "dividStocksPs")), KlineParser.Dec(get(r, "dividReserveToStockPs")),
                detailJson,   // 字符串，UpsertAsync 用 ::jsonb 转换
            });
        }
        return rows;
    }
}

/// <summary>分红落盘（upsert + 水位，同事务）。移植 fetch_dividend 的水位口径。</summary>
public interface IDividendWriter
{
    Task<int> PersistAsync(string code, int year, string yearType, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default);
}

public sealed class DividendWriter(StockDataDbContext db) : IDividendWriter
{
    public async Task<int> PersistAsync(string code, int year, string yearType, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
    {
        var rows = DividendParser.Parse(payload, code, year, yearType);
        // 按主键去重 + 字段级 merge：PG 单条 INSERT + ON CONFLICT 不允许同命令二次影响同一行
        // （cardinality_violation 21000）。baostock 对部分票/年返回重复行——大多数是完全相同
        // 的纯重复，少数是"近重复"（同 plan_announce_date 但某条缺字段、另一条完整，如 sz.002338
        // 2021：row[0] dividCashPsBeforeTax 为空、row[1] 有 0.08）。简单 g.Last() 碰巧能 work
        // （baostock 恰好把完整行放后面），但依赖行序、不健壮；改字段级 merge 保证无论行序如何
        // 都得最完整记录。PK=(code, plan_announce_date, year_type)，同调用 code/year/yearType
        // 恒定，按 plan_announce_date 分组即可。
        rows = rows
            .GroupBy(r => r[1])
            .Select(g => MergeGroup(g.ToList()))
            .ToList();
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var n = rows.Count == 0 ? 0 : await UpsertAsync(rows, ct);

        // 水位：last = min(year-12-31, 今天)，first = year-01-01（点状年覆盖，空结果也推进）
        var today = Coverage.Today(now);
        var last = new DateOnly(year, 12, 31) < today ? new DateOnly(year, 12, 31) : today;
        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES (@code,'dividend',@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date = LEAST(data_watermark.first_date, EXCLUDED.first_date),
                last_date  = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("code", code),
            new NpgsqlParameter("first", new DateOnly(year, 1, 1)), new NpgsqlParameter("last", last));

        await tx.CommitAsync(ct);
        return n;
    }

    private async Task<int> UpsertAsync(List<object?[]> rows, CancellationToken ct)
    {
        var cols = DividendParser.Columns;
        var pk = new[] { "code", "plan_announce_date", "year_type" };
        var sb = new System.Text.StringBuilder($"INSERT INTO dividend ({string.Join(",", cols)},updated_at) VALUES ");
        var ps = new List<NpgsqlParameter>();
        var pi = 0;
        for (var r = 0; r < rows.Count; r++)
        {
            if (r > 0) sb.Append(',');
            sb.Append('(');
            for (var c = 0; c < cols.Length; c++)
            {
                if (c > 0) sb.Append(',');
                if (cols[c] == "detail") sb.Append($"@p{pi}::jsonb"); else sb.Append($"@p{pi}");
                ps.Add(new NpgsqlParameter($"p{pi}", rows[r][c] ?? DBNull.Value));
                pi++;
            }
            sb.Append(",now())");
        }
        var setCols = cols.Where(c => !pk.Contains(c)).Select(c => $"{c}=EXCLUDED.{c}").Append("updated_at=now()");
        sb.Append($" ON CONFLICT ({string.Join(",", pk)}) DO UPDATE SET ").Append(string.Join(",", setCols));
        return await db.Database.ExecuteSqlRawAsync(sb.ToString(), ps, ct);
    }

    /// <summary>
    /// 同主键多行合并为一条：逐字段取非空值。处理 baostock 近重复行（同 plan_announce_date
    /// 但某行缺字段、另一行完整）。PK 列（code/plan_announce_date/year_type）各组相同，
    /// merge 实际只影响非空替换；detail JSONB 同理——首条非空即保留。
    /// </summary>
    private static object?[] MergeGroup(List<object?[]> group)
    {
        if (group.Count == 1) return group[0];
        var merged = (object?[])group[0].Clone();
        foreach (var row in group.Skip(1))
            for (var c = 0; c < merged.Length; c++)
                if (merged[c] is null or DBNull)
                    merged[c] = row[c];
        return merged;
    }
}

/// <summary>分红编排（范围类，按年）：CheckRange 判定该年是否需抓 → fetch {code,year,year_type} → 落盘。</summary>
public sealed class DividendService(IWatermarkStore watermarks, IFetchClient fetch, IDividendWriter writer)
{
    public async Task EnsureAsync(string code, int year, string yearType, DateTimeOffset now, CancellationToken ct = default)
    {
        var start = new DateOnly(year, 1, 1);
        var today = Coverage.Today(now);
        var end = new DateOnly(year, 12, 31) < today ? new DateOnly(year, 12, 31) : today;

        var wm = await watermarks.GetAsync(code, "dividend", ct);
        var decision = Coverage.CheckRange(wm?.ToWatermark(), "dividend", start, end, now);
        if (decision.Fresh) return;

        var payload = await fetch.FetchAsync(
            new FetchRequest("fetch_dividend", Code: code, Year: year.ToString(), YearType: yearType), ct);
        await writer.PersistAsync(code, year, yearType, payload, now, ct);
    }
}
