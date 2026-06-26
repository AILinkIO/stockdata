using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// 常驻同步消费者（TASK 本轮，方案 A）：唯一串行驱动 baostock 的后台 worker。
///
/// 循环从 stock_sync_task 取一个 due（pending/partial）任务 → SyncStockAsync/SyncMinuteAsync
/// （low 优先 fetch）→ 重复；空队列 sleep。**单 BackgroundService 实例 = 串行**，配合 fetch 的
/// 高/低优先队列：MCP 交互读的 high 任务在 Python 侧插队，后台全量是 low。
/// halt（baostock 拉黑/熔断）时暂停提交（由 FetchHaltMonitor 自动 /restart 恢复）。
/// cron 只「生成队列」（/sync/refresh 把过期票重置 pending）+ 立即返回；抓取全在本 worker。
/// </summary>
public sealed class SyncDrainer(
    IServiceProvider root, IConfiguration config, ILogger<SyncDrainer> logger,
    StockSyncService stockSync, SyncMarketService market, IFetchControl control) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var idle = TimeSpan.FromSeconds(config.GetValue("StockData:Sync:DrainIdleSeconds", 10));
        var haltBackoff = TimeSpan.FromSeconds(config.GetValue("StockData:Sync:HaltBackoffSeconds", 120));
        var marketEvery = TimeSpan.FromSeconds(config.GetValue("StockData:Sync:MarketRefreshSeconds", 3600));
        var time = root.GetRequiredService<TimeProvider>();
        var lastMarket = DateTimeOffset.MinValue;
        logger.LogInformation("SyncDrainer 启动（串行消费 stock_sync_task）");
        await ReclaimOrphanedAsync(ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // halt 期间不提交（避免 fetch 排队空等到超时）；等 FetchHaltMonitor 冷却后 /restart
                var st = await control.GetStatusAsync(ct);
                if (st?.IsHalted == true) { await Task.Delay(haltBackoff, ct); continue; }

                // 按间隔自维护市场级数据（无论队列空否，日期运算前置）→ cron/手动无需再碰市场
                var now = time.GetUtcNow();
                if (now - lastMarket > marketEvery) { await market.SyncMarketAsync(ct); lastMarket = now; }

                var next = await NextDueAsync(ct);
                if (next is null) { await Task.Delay(idle, ct); continue; }

                var o = next.Value.Kind == "minute"
                    ? await stockSync.SyncMinuteAsync(next.Value.Code, ct)
                    : await stockSync.SyncStockAsync(next.Value.Code, ct);
                if (o.Status == "partial") await Task.Delay(haltBackoff, ct);  // 多为 halt：退避等恢复
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)   // 防御：单次异常不打死循环
            {
                logger.LogWarning(ex, "SyncDrainer 循环异常，退避后继续");
                await Task.Delay(idle, ct);
            }
        }
        logger.LogInformation("SyncDrainer 停止");
    }

    /// <summary>启动回收：单 Drainer 串行 → 启动时无合法 running，遗留的都是上个进程中断的孤儿，
    /// 重置为 partial（续传保留 datasets_done），避免永久卡死被跳过。</summary>
    private async Task ReclaimOrphanedAsync(CancellationToken ct)
    {
        await using var scope = root.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
        var n = await db.Database.ExecuteSqlRawAsync(
            "UPDATE stock_sync_task SET status = 'partial', updated_at = now() WHERE status = 'running'", ct);
        if (n > 0) logger.LogInformation("回收 {N} 个孤儿 running 任务为 partial（续传）", n);
    }

    private async Task<(string Code, string Kind)?> NextDueAsync(CancellationToken ct)
    {
        await using var scope = root.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
        var t = await db.StockSyncTasks.AsNoTracking()
            .Where(x => x.Status == "pending" || x.Status == "partial")
            .OrderBy(x => x.UpdatedAt)
            .Select(x => new { x.Code, x.Kind })
            .FirstOrDefaultAsync(ct);
        return t is null ? null : (t.Code, t.Kind);
    }
}
