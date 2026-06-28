using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// 一次性 drain 消费者：循环从 stock_sync_task 取一个 due（pending/partial）任务 →
/// SyncStockAsync/SyncMinuteAsync（low 优先 fetch）→ 重复；队列空 / fetch halt 时退出。
///
/// 设计取舍：
/// - **单实例 = 串行**：配合 fetch 的高/低优先队列（MCP 交互读的 high 任务在 Python 侧插队）。
/// - **启动回收**：<see cref="ReclaimOrphanedAsync"/> 把遗留的 running 重置为 partial（续传保
///   datasets_done），避免单实例被永久卡死。
/// - **不依赖唤醒信号**：常驻版本用 SyncWakeUp 让 RegisterIfNew 触发立即唤醒；
///   一次性模式下 sync-cli 进程级生命周期由 cron 控制，cron 不查库、RegisterIfNew 落库后下次
///   cron 触发时 NextDueAsync 自然捡到，无需进程内唤醒通道。
/// - **halt 透传退出**：halt 不是局部故障，是 baostock IP 级拉黑——一次性 drain 没必要硬等冷却，
///   退出让外层 cron 在下个周期重试（中间空挡由 halt monitor 自动 /restart 拉起 fetch）。
/// - **市场级数据**：lastMarket 在 RunAsync 生命周期内单次刷新（start 触发一次）——一次性语义
///   下 cron 间不必自维护，下次启动再刷。
/// </summary>
public sealed class SyncDrainer(
    IServiceProvider root, IConfiguration config, ILogger<SyncDrainer> logger,
    StockSyncService stockSync, SyncMarketService market, IFetchControl control)
{
    public async Task RunAsync(CancellationToken ct)
    {
        var marketEvery = TimeSpan.FromSeconds(config.GetValue("StockData:Sync:MarketRefreshSeconds", 3600));
        var time = root.GetRequiredService<TimeProvider>();
        var lastMarket = DateTimeOffset.MinValue;
        logger.LogInformation("SyncDrainer 启动（一次性 drain）");
        await ReclaimOrphanedAsync(ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var st = await control.GetStatusAsync(ct);
                if (st?.IsHalted == true)
                {
                    logger.LogWarning("fetch halted，drain 提前结束（下次 cron 触发再续）");
                    break;
                }

                var now = time.GetUtcNow();
                if (now - lastMarket > marketEvery) { await market.SyncMarketAsync(ct); lastMarket = now; }

                var next = await NextDueAsync(ct);
                if (next is null)
                {
                    logger.LogInformation("drain 完成：队列已空");
                    break;
                }

                _ = next.Value.Kind == "minute"
                    ? await stockSync.SyncMinuteAsync(next.Value.Code, ct)
                    : await stockSync.SyncStockAsync(next.Value.Code, ct);
                // partial/failed 不再退避：partial 多为 halt，下一轮 halt check 自然 break；
                // 其他失败已落库（status='failed'），下一轮 NextDueAsync 会跳过。
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "SyncDrainer 循环异常，10s 后继续");
                try { await Task.Delay(TimeSpan.FromSeconds(10), ct); }
                catch (OperationCanceledException) { break; }
            }
        }
        logger.LogInformation("SyncDrainer 退出");
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