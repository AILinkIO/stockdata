using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// 交易日历读取（dotnet 路径对外查询）：EnsureRange → 直读 PG → 序列化为旧 API 同形状
/// JSON 数组 [{calendar_date, is_trading_day}]（对齐 api/services/market.get_trade_calendar）。
/// 始终注册，<see cref="Enabled"/> 反映开关；关闭时工具走旧 REST，对现网零影响。
/// </summary>
public sealed class TradeCalendarReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public Task<string> GetJsonAsync(DateOnly start, DateOnly end, CancellationToken ct = default)
        => SyncAwaiter.GuardAsync(async () =>
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var svc = sp.GetRequiredService<TradeCalendarService>();
        var time = sp.GetRequiredService<TimeProvider>();

        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, "", "trade_calendar", start, end, tp.GetUtcNow()),
            c => svc.EnsureRangeAsync(start, end, tp.GetUtcNow(), c));

        var rows = await db.TradeCalendars.AsNoTracking()
            .Where(c => c.CalendarDate >= start && c.CalendarDate <= end)
            .OrderBy(c => c.CalendarDate)
            .ToListAsync(ct);

        return Serialize(rows);
    });

    internal static string Serialize(IReadOnlyList<TradeCalendar> rows)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartArray();
            foreach (var c in rows)
            {
                w.WriteStartObject();
                w.WriteString("calendar_date", c.CalendarDate.ToString("yyyy-MM-dd"));
                w.WriteBoolean("is_trading_day", c.IsTradingDay);
                w.WriteEndObject();
            }
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
