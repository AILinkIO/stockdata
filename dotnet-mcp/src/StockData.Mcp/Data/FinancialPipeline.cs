using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>财报一行：report_type 列化，stat_date/pub_date 提列，其余进 metrics(JSONB)。</summary>
public readonly record struct FinRow(string ReportType, DateOnly StatDate, DateOnly? PubDate, string MetricsJson);

/// <summary>
/// 财报解析（移植 write_financial_reports）。季度类 payload 非表格：fields=[report_type, record]，
/// record 是该类的 JSON 记录 → 提 statDate/pubDate、其余进 metrics。快报/预告是表格，stat/pub key 各异。
/// </summary>
public static class FinancialParser
{
    private static readonly JsonSerializerOptions Relaxed = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public static List<FinRow> ParseQuarterly(FetchPayload payload)
    {
        var get = SnapshotSql.Accessor(payload);
        var result = new List<FinRow>();
        foreach (var r in payload.Rows)
        {
            var reportType = get(r, "report_type");
            var record = get(r, "record");
            if (reportType is null || record is null) continue;
            if (FromRecordJson(reportType, record, "statDate", "pubDate") is FinRow fr) result.Add(fr);
        }
        return result;
    }

    public static List<FinRow> ParsePerformance(FetchPayload payload, string reportType, string statKey, string pubKey)
    {
        var result = new List<FinRow>();
        foreach (var r in payload.Rows)
        {
            DateOnly? stat = null, pub = null;
            var metrics = new Dictionary<string, string?>();
            for (var i = 0; i < payload.Fields.Count && i < r.Count; i++)
            {
                var f = payload.Fields[i];
                if (f == statKey) stat = KlineParser.Date(r[i]);
                else if (f == pubKey) pub = KlineParser.Date(r[i]);
                else if (f != "code") metrics[f] = r[i];
            }
            if (stat is DateOnly s) result.Add(new FinRow(reportType, s, pub, JsonSerializer.Serialize(metrics, Relaxed)));
        }
        return result;
    }

    private static FinRow? FromRecordJson(string reportType, string recordJson, string statKey, string pubKey)
    {
        using var doc = JsonDocument.Parse(recordJson);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object) return null;

        if (!(root.TryGetProperty(statKey, out var sd) && sd.ValueKind == JsonValueKind.String
              && KlineParser.Date(sd.GetString()) is DateOnly stat)) return null;
        DateOnly? pub = root.TryGetProperty(pubKey, out var pd) && pd.ValueKind == JsonValueKind.String
            ? KlineParser.Date(pd.GetString()) : null;

        var metrics = new Dictionary<string, JsonElement>();
        foreach (var prop in root.EnumerateObject())
            if (prop.Name != "code" && prop.Name != statKey && prop.Name != pubKey) metrics[prop.Name] = prop.Value;

        return new FinRow(reportType, stat, pub, JsonSerializer.Serialize(metrics, Relaxed));
    }
}

public interface IFinancialWriter
{
    Task PersistQuarterlyAsync(string code, int year, int quarter, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default);
    Task PersistPerformanceAsync(string code, string reportType, DateOnly start, DateOnly end, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default);
}

public sealed class FinancialWriter(StockDataDbContext db) : IFinancialWriter
{
    // express/forecast 的 stat/pub 字段名（baostock）
    public static (string Stat, string Pub) PerfKeys(string reportType) => reportType == "express"
        ? ("performanceExpStatDate", "performanceExpPubDate")
        : ("profitForcastExpStatDate", "profitForcastExpPubDate");

    public async Task PersistQuarterlyAsync(string code, int year, int quarter, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
    {
        var rows = FinancialParser.ParseQuarterly(payload);
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        await UpsertAsync(code, rows, ct);
        // 季度水位（负结果记忆）：合成 data_type，last_fetched_at = last_success；空结果也写
        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,last_date,last_fetched_at)
            VALUES (@c,@d,@l,now())
            ON CONFLICT (code,data_type) DO UPDATE SET last_date=EXCLUDED.last_date, last_fetched_at=now()
            """,
            new NpgsqlParameter("c", code), new NpgsqlParameter("d", $"fin:{year}q{quarter}"),
            new NpgsqlParameter("l", Coverage.QuarterEnd(year, quarter)));
        await tx.CommitAsync(ct);
    }

    public async Task PersistPerformanceAsync(string code, string reportType, DateOnly start, DateOnly end, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
    {
        var (statKey, pubKey) = PerfKeys(reportType);
        var rows = FinancialParser.ParsePerformance(payload, reportType, statKey, pubKey);
        var last = Coverage.ClaimableLast(reportType, end, null, Coverage.Today(now));

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        await UpsertAsync(code, rows, ct);
        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES (@c,@d,@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date=LEAST(data_watermark.first_date,EXCLUDED.first_date),
                last_date=GREATEST(data_watermark.last_date,EXCLUDED.last_date), last_fetched_at=now()
            """,
            new NpgsqlParameter("c", code), new NpgsqlParameter("d", reportType),
            new NpgsqlParameter("first", start), new NpgsqlParameter("last", last));
        await tx.CommitAsync(ct);
    }

    private async Task UpsertAsync(string code, List<FinRow> rows, CancellationToken ct)
    {
        if (rows.Count == 0) return;
        // 按主键去重：PG 单条 INSERT + ON CONFLICT 不允许同命令二次影响同一行（cardinality_violation 21000）
        rows = rows
            .GroupBy(r => (r.ReportType, r.StatDate))
            .Select(g => g.Last())
            .ToList();
        var sb = new StringBuilder(
            "INSERT INTO financial_report (code,report_type,stat_date,pub_date,metrics,updated_at) VALUES ");
        var ps = new List<NpgsqlParameter>();
        var p = 0;
        for (var r = 0; r < rows.Count; r++)
        {
            if (r > 0) sb.Append(',');
            sb.Append($"(@p{p},@p{p + 1},@p{p + 2},@p{p + 3},@p{p + 4}::jsonb,now())");
            ps.Add(new($"p{p}", code)); ps.Add(new($"p{p + 1}", rows[r].ReportType));
            ps.Add(new($"p{p + 2}", rows[r].StatDate));
            ps.Add(new($"p{p + 3}", (object?)rows[r].PubDate ?? DBNull.Value));
            ps.Add(new($"p{p + 4}", rows[r].MetricsJson));
            p += 5;
        }
        sb.Append(" ON CONFLICT (code,report_type,stat_date) DO UPDATE SET ")
          .Append("pub_date=EXCLUDED.pub_date,metrics=EXCLUDED.metrics,updated_at=now()");
        await db.Database.ExecuteSqlRawAsync(sb.ToString(), ps, ct);
    }
}

/// <summary>季度财报编排（CheckQuarter 点状）：has_rows + last_success(合成水位) → 判定 → 抓取 → 落盘。</summary>
public sealed class FinancialQuarterService(StockDataDbContext db, IFetchClient fetch, IWatermarkStore watermarks, IFinancialWriter writer)
{
    public async Task EnsureAsync(string code, int year, int quarter, DateTimeOffset now, CancellationToken ct = default)
    {
        var statDate = Coverage.QuarterEnd(year, quarter);
        var hasRows = await db.FinancialReports.AsNoTracking().AnyAsync(r => r.Code == code && r.StatDate == statDate, ct);
        var wm = await watermarks.GetAsync(code, $"fin:{year}q{quarter}", ct);
        var decision = Coverage.CheckQuarter(hasRows, wm?.LastFetchedAt, year, quarter, now);
        if (decision.Fresh) return;

        var payload = await fetch.FetchAsync(
            new FetchRequest("fetch_financial_report", Code: code, Year: year.ToString(), Quarter: quarter), ct);
        await writer.PersistQuarterlyAsync(code, year, quarter, payload, now, ct);
    }
}

/// <summary>业绩快报/预告编排（CheckRange 范围，按 pub_date）。</summary>
public sealed class PerformanceService(IFetchClient fetch, IWatermarkStore watermarks, IFinancialWriter writer)
{
    public async Task EnsureAsync(string code, string reportType, DateOnly start, DateOnly end, DateTimeOffset now, CancellationToken ct = default)
    {
        var wm = await watermarks.GetAsync(code, reportType, ct);
        var decision = Coverage.CheckRange(wm?.ToWatermark(), reportType, start, end, now);
        if (decision.Fresh) return;

        var payload = await fetch.FetchAsync(
            new FetchRequest("fetch_performance", Code: code, ReportType: reportType, StartDate: start, EndDate: end), ct);
        await writer.PersistPerformanceAsync(code, reportType, start, end, payload, now, ct);
    }
}
