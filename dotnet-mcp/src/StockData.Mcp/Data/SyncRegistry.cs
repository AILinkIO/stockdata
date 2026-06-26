using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace StockData.Mcp.Data;

/// <summary>
/// 懒加载登记（TASK 本轮 ②⑤）：读到一个未纳管的 code 时，把它登记进 <c>synced_stock</c>
/// 并建一条 pending 的 <c>stock_sync_task</c>（kind=full）。两步幂等（ON CONFLICT DO NOTHING），
/// 单条 CTE 一次往返、不触 baostock，故可在读路径内安全调用（仅 ServeFromPgOnly 模式下）。
/// 真正的抓取由命令式同步（/sync/run 等，P2）按这两张表续传完成。
/// </summary>
internal static class SyncRegistry
{
    private const string Sql = """
        WITH s AS (
            INSERT INTO synced_stock (code, first_seen_at, minute_enabled, updated_at)
            VALUES (@c, now(), false, now())
            ON CONFLICT (code) DO NOTHING
        )
        INSERT INTO stock_sync_task (code, kind, status, datasets_done, requested_at, attempt, updated_at)
        VALUES (@c, 'full', 'pending', ARRAY[]::text[], now(), 0, now())
        ON CONFLICT (code, kind) DO NOTHING
        """;

    /// <summary>登记单个 code（已存在则无操作）。空 code 跳过。复用调用方已开的 DbContext。</summary>
    public static Task RegisterIfNewAsync(StockDataDbContext db, string code, CancellationToken ct = default)
        => string.IsNullOrEmpty(code)
            ? Task.CompletedTask
            : db.Database.ExecuteSqlRawAsync(Sql, new[] { new NpgsqlParameter("c", code) }, ct);

    /// <summary>标记某票已纳管分钟线（显式分钟线同步时置位；行须先经 RegisterIfNew 存在）。</summary>
    public static Task EnableMinuteAsync(StockDataDbContext db, string code, CancellationToken ct = default)
        => db.Database.ExecuteSqlRawAsync(
            "UPDATE synced_stock SET minute_enabled = true, updated_at = now() WHERE code = @c",
            new[] { new NpgsqlParameter("c", code) }, ct);
}
