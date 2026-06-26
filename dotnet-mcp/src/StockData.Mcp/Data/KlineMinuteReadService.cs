using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data;

/// <summary>
/// 分钟 K 线读取（EnsureRange → 直读 PG，用 PG json_agg 通用序列化全列）。
/// bar_time 右开区间 [start 00:00+08, (end+1) 00:00+08)（与旧 get_kline_minute 一致）。
/// 始终注册，Enabled 反映开关。
/// </summary>
public sealed class KlineMinuteReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public async Task<string> GetJsonAsync(string code, short frequency, DateOnly start, DateOnly end, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        // ServeFromPgOnly：纯 PG 读，仅登记票（分钟数据由显式分钟线同步任务喂，P3）
        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        else await sp.GetRequiredService<KlineMinuteService>().EnsureRangeAsync(code, frequency, start, end, now, ct);

        var lo = new DateTimeOffset(start.Year, start.Month, start.Day, 0, 0, 0, TimeSpan.FromHours(8));
        var hi = new DateTimeOffset(end.Year, end.Month, end.Day, 0, 0, 0, TimeSpan.FromHours(8)).AddDays(1);
        const string sql =
            "SELECT COALESCE(json_agg(t ORDER BY t.bar_time), '[]')::text AS \"Value\" FROM kline_minute t " +
            "WHERE t.code = {0} AND t.frequency = {1} AND t.bar_time >= {2} AND t.bar_time < {3}";
        return await db.Database.SqlQueryRaw<string>(sql, code, (int)frequency, lo.ToUniversalTime(), hi.ToUniversalTime()).FirstAsync(ct);
    }
}
