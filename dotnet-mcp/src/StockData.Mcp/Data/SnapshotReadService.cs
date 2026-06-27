using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data;

/// <summary>
/// 快照三件套读取（stock_list / index_constituent / industry）。snap_date 缺省解析为最近交易日；
/// stock_list 当日未发布时回退前一交易日（最多 4 次，移植 get_stock_list allow_fallback）。
/// 用 PG json_build_object 精确列（排除 updated_at，对齐旧 _rows 列表）。始终注册，Enabled 反映开关。
/// </summary>
public sealed class SnapshotReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    // 快照三件套均为市场级数据（无单票 code），ServeFromPgOnly 下纯读，缺口由 /sync/market 补（P2）
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public async Task<string> StockListJsonAsync(DateOnly? snapDate, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var snaps = sp.GetRequiredService<SnapshotService>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();

        var allowFallback = snapDate is null;
        if (await Resolve(sp, snapDate, now, ServeFromPgOnly, ct) is not DateOnly sd) return "[]";

        const string sql =
            "SELECT COALESCE(json_agg(json_build_object('snap_date',t.snap_date,'code',t.code," +
            "'code_name',t.code_name,'trade_status',t.trade_status) ORDER BY t.code),'[]')::text AS \"Value\" " +
            "FROM stock_list_snapshot t WHERE t.snap_date = {0}";

        for (var i = 0; i < (allowFallback ? 4 : 1); i++)
        {
            await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
                SyncAwaiter.SnapshotCheck(watermarks, "", "stock_list", sd, now),
                c => snaps.EnsureSnapshotAsync(new StockListIngest(db), sd, now, c));
            var json = await db.Database.SqlQueryRaw<string>(sql, sd).FirstAsync(ct);
            if (json != "[]" || !allowFallback) return json;
            if (await PreviousTradingDay(sp, sd, now, ServeFromPgOnly, ct) is not DateOnly prev) return json;
            sd = prev;
        }
        return "[]";
    }

    public async Task<string> IndexConstituentsJsonAsync(string indexCode, DateOnly? snapDate, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        if (await Resolve(sp, snapDate, now, ServeFromPgOnly, ct) is not DateOnly sd) return "[]";

        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.SnapshotCheck(watermarks, "", $"index_{indexCode}", sd, now),
            c => sp.GetRequiredService<SnapshotService>().EnsureSnapshotAsync(new IndexConstituentIngest(db, indexCode), sd, now, c));

        const string sql =
            "SELECT COALESCE(json_agg(json_build_object('index_code',t.index_code,'snap_date',t.snap_date," +
            "'code',t.code,'code_name',t.code_name) ORDER BY t.code),'[]')::text AS \"Value\" " +
            "FROM index_constituent t WHERE t.index_code = {0} AND t.snap_date = {1}";
        return await db.Database.SqlQueryRaw<string>(sql, indexCode, sd).FirstAsync(ct);
    }

    public async Task<string> IndustryJsonAsync(string? code, DateOnly? snapDate, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var watermarks = sp.GetRequiredService<IWatermarkStore>();
        var tp = sp.GetRequiredService<TimeProvider>();
        if (await Resolve(sp, snapDate, now, ServeFromPgOnly, ct) is not DateOnly sd) return "[]";

        await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, null, tp, ct,
            SyncAwaiter.SnapshotCheck(watermarks, "", "industry", sd, now),
            c => sp.GetRequiredService<SnapshotService>().EnsureSnapshotAsync(new IndustryIngest(db), sd, now, c));

        var sql =
            "SELECT COALESCE(json_agg(json_build_object('snap_date',t.snap_date,'code',t.code,'code_name',t.code_name," +
            "'industry',t.industry,'industry_classification',t.industry_classification) ORDER BY t.code),'[]')::text AS \"Value\" " +
            "FROM stock_industry t WHERE t.snap_date = {0}" + (code is null ? "" : " AND t.code = {1}");
        return code is null
            ? await db.Database.SqlQueryRaw<string>(sql, sd).FirstAsync(ct)
            : await db.Database.SqlQueryRaw<string>(sql, sd, code).FirstAsync(ct);
    }

    // snap_date 缺省 → 最近交易日（≤今天）；显式给定则原样。pgOnly 下不补日历（靠 /sync/market）
    private static async Task<DateOnly?> Resolve(IServiceProvider sp, DateOnly? snapDate, DateTimeOffset now, bool pgOnly, CancellationToken ct)
    {
        if (snapDate is DateOnly d) return d;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var today = Coverage.Today(now);
        if (!pgOnly) await sp.GetRequiredService<TradeCalendarService>().EnsureRangeAsync(today.AddDays(-45), today, now, ct);
        return await db.TradeCalendars.AsNoTracking()
            .Where(c => c.CalendarDate <= today && c.IsTradingDay)
            .OrderByDescending(c => c.CalendarDate).Select(c => (DateOnly?)c.CalendarDate).FirstOrDefaultAsync(ct);
    }

    private static async Task<DateOnly?> PreviousTradingDay(IServiceProvider sp, DateOnly d, DateTimeOffset now, bool pgOnly, CancellationToken ct)
    {
        var db = sp.GetRequiredService<StockDataDbContext>();
        if (!pgOnly) await sp.GetRequiredService<TradeCalendarService>().EnsureRangeAsync(d.AddDays(-45), d.AddDays(-1), now, ct);
        return await db.TradeCalendars.AsNoTracking()
            .Where(c => c.CalendarDate < d && c.IsTradingDay)
            .OrderByDescending(c => c.CalendarDate).Select(c => (DateOnly?)c.CalendarDate).FirstOrDefaultAsync(ct);
    }
}
