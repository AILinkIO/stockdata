using System.Buffers;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// 股票基本信息读取（快照 serving 样板，snap_date=今天，无日历依赖）：
/// EnsureSnapshot → 直读 PG → 序列化为旧 API 同形状单对象 {code,code_name,ipo_date,out_date,type,status}
/// （对齐 market.get_stock_basic；无数据返回 "null"）。始终注册，Enabled 反映开关。
/// </summary>
public sealed class StockBasicReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public Task<string> GetJsonAsync(string code, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var snap = sp.GetRequiredService<SnapshotService>();

        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var now = tp.GetUtcNow();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.SnapshotCheck(watermarks, code, "stock_basic", Coverage.Today(now), now),
            c => snap.EnsureSnapshotAsync(new StockBasicIngest(db, code), Coverage.Today(now), now, c));

        var r = await db.StockBasics.AsNoTracking().FirstOrDefaultAsync(x => x.Code == code, ct);
        return r is null ? "null" : Serialize(r);
    });

    internal static string Serialize(StockBasic r)
    {
        var buffer = new ArrayBufferWriter<byte>();
        // 中文(code_name)按 UTF-8 原样输出，对齐 Python Starlette(ensure_ascii=False)
        using (var w = new Utf8JsonWriter(buffer, new JsonWriterOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            w.WriteStartObject();
            w.WriteString("code", r.Code);
            if (r.CodeName is null) w.WriteNull("code_name"); else w.WriteString("code_name", r.CodeName);
            WriteDate(w, "ipo_date", r.IpoDate);
            WriteDate(w, "out_date", r.OutDate);
            if (r.Type is short t) w.WriteNumber("type", t); else w.WriteNull("type");
            if (r.Status is short s) w.WriteNumber("status", s); else w.WriteNull("status");
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void WriteDate(Utf8JsonWriter w, string name, DateOnly? d)
    {
        if (d is DateOnly v) w.WriteString(name, v.ToString("yyyy-MM-dd")); else w.WriteNull(name);
    }
}
