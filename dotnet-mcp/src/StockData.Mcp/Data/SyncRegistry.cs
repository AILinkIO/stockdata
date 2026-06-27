using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace StockData.Mcp.Data;

/// <summary>
/// 懒加载登记 + stale 重排（TASK 本轮 ②⑤）：读到 code 时登记进 synced_stock + stock_sync_task。
/// 新票 → 插入 pending 任务；已纳管且 task=done → 重置 pending（触发 Drainer 重新检查，
/// Coverage 保证仅刷新 stale 的数据类型）；task 非 done → 无操作。
/// </summary>
internal static class SyncRegistry
{
    private static SyncWakeUp? _wakeUp;

    public static void Configure(SyncWakeUp wakeUp) => _wakeUp = wakeUp;

    private const string Sql = """
        WITH s AS (
            INSERT INTO synced_stock (code, first_seen_at, minute_enabled, updated_at)
            VALUES (@c, now(), false, now())
            ON CONFLICT (code) DO NOTHING
        )
        INSERT INTO stock_sync_task (code, kind, status, datasets_done, requested_at, attempt, updated_at)
        VALUES (@c, 'full', 'pending', ARRAY[]::text[], now(), 0, now())
        ON CONFLICT (code, kind) DO UPDATE SET
            status = 'pending', datasets_done = ARRAY[]::text[], updated_at = now()
        WHERE stock_sync_task.status = 'done'
        """;

    /// <summary>登记单个 code。新票插入 pending 任务；done 状态的重置 pending 触发 Drainer 刷新。空 code 跳过。</summary>
    public static async Task RegisterIfNewAsync(StockDataDbContext db, string code, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(code)) return;
        var n = await db.Database.ExecuteSqlRawAsync(Sql, new[] { new NpgsqlParameter("c", code) }, ct);
        if (n > 0) _wakeUp?.Signal();
    }

    /// <summary>标记某票已纳管分钟线（显式分钟线同步时置位；行须先经 RegisterIfNew 存在）。</summary>
    public static Task EnableMinuteAsync(StockDataDbContext db, string code, CancellationToken ct = default)
        => db.Database.ExecuteSqlRawAsync(
            "UPDATE synced_stock SET minute_enabled = true, updated_at = now() WHERE code = @c",
            new[] { new NpgsqlParameter("c", code) }, ct);
}
