using System.Buffers;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// 财报读取（移植 api/services/financial.py）：季度六类、综合指标合并、快报/预告。
/// metrics(JSONB) 在季度类作嵌套对象输出；快报/综合指标里展平进行。始终注册，Enabled 反映开关。
/// </summary>
public sealed class FinancialReadService(IServiceProvider root, IConfiguration config)
{
    public static readonly string[] QuarterlyTypes = { "profit", "operation", "growth", "balance", "cash_flow", "dupont" };
    private static readonly JsonWriterOptions Wo = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public Task<string> GetQuarterlyJsonAsync(string code, int year, int quarter, string? reportType, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var now = tp.GetUtcNow();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.QuarterCheck(watermarks, db, code, year, quarter, now),
            c => sp.GetRequiredService<FinancialQuarterService>().EnsureAsync(code, year, quarter, now, c));

        var rows = await ReadQuarter(db, code, year, quarter, reportType, ct);
        var buf = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buf, Wo))
        {
            w.WriteStartArray();
            foreach (var r in rows) WriteQuarterly(w, r);
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    });

    public Task<string> GetPerformanceJsonAsync(string code, string reportType, DateOnly start, DateOnly end, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var now = tp.GetUtcNow();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, code, reportType, start, end, now),
            c => sp.GetRequiredService<PerformanceService>().EnsureAsync(code, reportType, start, end, now, c));

        var rows = await db.FinancialReports.AsNoTracking()
            .Where(r => r.Code == code && r.ReportType == reportType && r.PubDate >= start && r.PubDate <= end)
            .OrderBy(r => r.StatDate).ToListAsync(ct);

        var buf = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buf, Wo))
        {
            w.WriteStartArray();
            foreach (var r in rows)
            {
                w.WriteStartObject();
                w.WriteString("code", r.Code);
                WriteDate(w, "stat_date", r.StatDate);
                WriteDate(w, "pub_date", r.PubDate);
                SpreadMetrics(w, r.Metrics);          // metrics 字段展平
                w.WriteEndObject();
            }
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    });

    public Task<string> GetIndicatorJsonAsync(string code, DateOnly start, DateOnly end, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var quarterSvc = sp.GetRequiredService<FinancialQuarterService>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);

        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();

        var buf = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buf, Wo))
        {
            w.WriteStartArray();
            for (var year = start.Year; year <= end.Year; year++)
                for (var quarter = 1; quarter <= 4; quarter++)
                {
                    var qStart = new DateOnly(year, (quarter - 1) * 3 + 1, 1);
                    if (qStart > end || Coverage.QuarterEnd(year, quarter) < start) continue;
                    await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
                        SyncAwaiter.QuarterCheck(watermarks, db, code, year, quarter, now),
                        c => quarterSvc.EnsureAsync(code, year, quarter, now, c));
                    var rows = await ReadQuarter(db, code, year, quarter, null, ct);
                    if (rows.Count == 0) continue;

                    w.WriteStartObject();
                    w.WriteString("code", code);
                    w.WriteNumber("year", year);
                    w.WriteNumber("quarter", quarter);
                    foreach (var r in rows) SpreadMetrics(w, r.Metrics, prefix: r.ReportType + "_");  // 字段加类别前缀
                    w.WriteEndObject();
                }
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buf.WrittenSpan);
    });

    private static Task<List<FinancialReport>> ReadQuarter(StockDataDbContext db, string code, int year, int quarter, string? reportType, CancellationToken ct)
    {
        var types = reportType is null ? QuarterlyTypes : new[] { reportType };
        var statDate = Coverage.QuarterEnd(year, quarter);
        return db.FinancialReports.AsNoTracking()
            .Where(r => r.Code == code && types.Contains(r.ReportType) && r.StatDate == statDate)
            .OrderBy(r => r.ReportType).ToListAsync(ct);
    }

    private static void WriteQuarterly(Utf8JsonWriter w, FinancialReport r)
    {
        w.WriteStartObject();
        w.WriteString("code", r.Code);
        w.WriteString("report_type", r.ReportType);
        WriteDate(w, "stat_date", r.StatDate);
        WriteDate(w, "pub_date", r.PubDate);
        w.WritePropertyName("metrics");
        w.WriteRawValue(string.IsNullOrEmpty(r.Metrics) ? "{}" : r.Metrics);  // 嵌套对象
        w.WriteEndObject();
    }

    private static void SpreadMetrics(Utf8JsonWriter w, string? metrics, string prefix = "")
    {
        if (string.IsNullOrEmpty(metrics)) return;
        using var doc = JsonDocument.Parse(metrics);
        if (doc.RootElement.ValueKind != JsonValueKind.Object) return;
        foreach (var p in doc.RootElement.EnumerateObject())
        {
            w.WritePropertyName(prefix + p.Name);
            p.Value.WriteTo(w);
        }
    }

    private static void WriteDate(Utf8JsonWriter w, string name, DateOnly? d)
    {
        if (d is DateOnly v) w.WriteString(name, v.ToString("yyyy-MM-dd")); else w.WriteNull(name);
    }
}
