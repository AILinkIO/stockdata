using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data;

/// <summary>
/// 宏观读取（EnsureRange → 直读 PG）。用 PG <c>json_agg(row)</c> 通用序列化全部列（snake_case，
/// 对齐旧 API model_columns 输出），免去逐表手写序列化。始终注册，Enabled 反映开关。
/// </summary>
public sealed class MacroReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    /// <summary>利率类（deposit_rate / loan_rate / rrr）：按 pub_date 范围。</summary>
    public async Task<string> GetRatesJsonAsync(string kind, DateOnly start, DateOnly end, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, "", kind, start, end, now),
            c => sp.GetRequiredService<MacroService>().EnsureRangeAsync(kind, start, end, now, c));

        var table = MacroSpecs.All[kind].Table;
        var sql = $"SELECT COALESCE(json_agg(t ORDER BY t.pub_date), '[]')::text AS \"Value\" " +
                  $"FROM {table} t WHERE t.pub_date >= {{0}} AND t.pub_date <= {{1}}";
        return await db.Database.SqlQueryRaw<string>(sql, start, end).FirstAsync(ct);
    }

    public async Task<string> GetMoneyMonthJsonAsync(DateOnly start, DateOnly end, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, "", "money_supply_month", start, end, now),
            c => sp.GetRequiredService<MacroService>().EnsureRangeAsync("money_supply_month", start, end, now, c));

        const string sql =
            "SELECT COALESCE(json_agg(t ORDER BY t.stat_year, t.stat_month), '[]')::text AS \"Value\" " +
            "FROM money_supply_month t " +
            "WHERE (t.stat_year, t.stat_month) >= ({0}, {1}) AND (t.stat_year, t.stat_month) <= ({2}, {3})";
        return await db.Database.SqlQueryRaw<string>(sql, start.Year, start.Month, end.Year, end.Month).FirstAsync(ct);
    }

    public async Task<string> GetMoneyYearJsonAsync(int startYear, int endYear, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var msStart = new DateOnly(startYear, 1, 1);
        var msEnd = new DateOnly(endYear, 12, 31);
        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.RangeCheck(watermarks, "", "money_supply_year", msStart, msEnd, now),
            c => sp.GetRequiredService<MacroService>().EnsureRangeAsync("money_supply_year", msStart, msEnd, now, c));

        const string sql =
            "SELECT COALESCE(json_agg(t ORDER BY t.stat_year), '[]')::text AS \"Value\" " +
            "FROM money_supply_year t WHERE t.stat_year >= {0} AND t.stat_year <= {1}";
        return await db.Database.SqlQueryRaw<string>(sql, startYear, endYear).FirstAsync(ct);
    }
}
