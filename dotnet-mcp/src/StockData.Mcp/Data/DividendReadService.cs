using System.Buffers;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// 分红读取（EnsureAsync → 直读 PG → 序列化为旧 API 同形状数组，对齐 market.get_dividends；
/// detail 作为嵌套 JSON 原样输出）。始终注册，Enabled 反映开关。
/// </summary>
public sealed class DividendReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public async Task<string> GetJsonAsync(string code, int year, string yearType, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var svc = sp.GetRequiredService<DividendService>();

        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var now = tp.GetUtcNow();
        var divStart = new DateOnly(year, 1, 1);
        var divEnd = new DateOnly(year, 12, 31);
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, code, "dividend", divStart, divEnd, now),
            c => svc.EnsureAsync(code, year, yearType, now, c));

        var rows = await db.Dividends.AsNoTracking()
            .Where(d => d.Code == code && d.Year == (short)year && d.YearType == yearType)
            .OrderBy(d => d.PlanAnnounceDate)
            .ToListAsync(ct);

        return Serialize(rows);
    }

    internal static string Serialize(IReadOnlyList<Dividend> rows)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            w.WriteStartArray();
            foreach (var d in rows)
            {
                w.WriteStartObject();
                w.WriteString("code", d.Code);
                w.WriteNumber("year", d.Year);
                w.WriteString("year_type", d.YearType);
                w.WriteString("plan_announce_date", d.PlanAnnounceDate.ToString("yyyy-MM-dd"));
                Date(w, "regist_date", d.RegistDate);
                Date(w, "operate_date", d.OperateDate);
                Date(w, "pay_date", d.PayDate);
                Dec(w, "cash_ps_before_tax", d.CashPsBeforeTax);
                Dec(w, "cash_ps_after_tax", d.CashPsAfterTax);
                Dec(w, "stocks_ps", d.StocksPs);
                Dec(w, "reserve_to_stock_ps", d.ReserveToStockPs);
                if (d.Detail is null) w.WriteNull("detail"); else { w.WritePropertyName("detail"); w.WriteRawValue(d.Detail); }
                w.WriteEndObject();
            }
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void Date(Utf8JsonWriter w, string n, DateOnly? d)
    {
        if (d is DateOnly v) w.WriteString(n, v.ToString("yyyy-MM-dd")); else w.WriteNull(n);
    }

    private static void Dec(Utf8JsonWriter w, string n, decimal? v)
    {
        if (v is decimal d) w.WriteNumber(n, d); else w.WriteNull(n);
    }
}
