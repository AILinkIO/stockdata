using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;
using StockData.SyncCli.Progress;

namespace StockData.SyncCli;

/// <summary>
/// CLI 形态的同步引擎：把 MCP 的 <see cref="SyncDrainer"/>（已抽成 plain class）包一层，外加
/// 一个并行 progress poller，每 1s 查一次 stock_sync_task 计数 + 当前 running task 的
/// datasets_done，通过 <see cref="IProgressSource"/> 发到 UI / 日志订阅者。
///
/// 设计取舍：
/// - **不侵入 SyncDrainer**：底层逻辑零修改，poller 独立跑 → MCP 单元测试不变；
/// - **解耦步级进度**：不依赖 StockSyncService 暴露 step hook，靠每 1s poll running task 的
///   datasets_done 数组拿到 L2（当前票的已完成步），够用且零侵入；
/// - **节流 1Hz**：poller 自己控制频率，不靠订阅方过滤。
/// </summary>
public sealed class SyncEngine
{
    private readonly SyncDrainer _drainer;
    private readonly IProgressSource _progress;
    private readonly IServiceProvider _root;
    private readonly IConfiguration _config;
    private readonly IFetchControl _control;
    private readonly ILogger<SyncEngine> _logger;
    private readonly TimeProvider _time;

    // 启动时刻：RunAsync 内赋值，供 ForcePollAsync 计算 Elapsed。
    // 进程内只有一个 SyncEngine 实例（DI 单例），无并发问题。
    private DateTimeOffset _startedAt;

    public SyncEngine(
        SyncDrainer drainer,
        IProgressSource progress,
        IServiceProvider root,
        IConfiguration config,
        IFetchControl control,
        ILogger<SyncEngine> logger,
        TimeProvider time)
    {
        _drainer = drainer;
        _progress = progress;
        _root = root;
        _config = config;
        _control = control;
        _logger = logger;
        _time = time;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        _startedAt = _time.GetUtcNow();
        var startedAt = _startedAt;
        _progress.Emit(SyncProgress.Empty with { Elapsed = TimeSpan.Zero });

        // 平行的 progress poller；与 drainer 并行。任何一个抛 OperationCanceledException
        // 视作正常退出。
        using var pollerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var poller = PollLoopAsync(startedAt, pollerCts.Token);

        try
        {
            await _drainer.RunAsync(ct);
        }
        finally
        {
            // drainer 退出（Ctrl-C 或自然完成：队列空 / halt）→ 先发一次终末快照，再通知 poller 收尾
            try
            {
                var final = await BuildSnapshotAsync(startedAt, CancellationToken.None);
                _progress.Emit(final);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "终末快照失败（忽略）");
            }
            pollerCts.Cancel();
            try { await poller; } catch (OperationCanceledException) { /* 正常 */ }
            _logger.LogInformation("SyncEngine 退出");
        }
    }

    /// <summary>
    /// 立即查一次进度快照并 emit（跳过 1Hz poller 等待）。供 DashboardWindow 在
    /// add stock / retry 等用户动作后强制刷新 dashboard 显示。
    /// </summary>
    public async Task ForcePollAsync()
    {
        try
        {
            var snapshot = await BuildSnapshotAsync(_startedAt, CancellationToken.None);
            _progress.Emit(snapshot);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "ForcePollAsync 失败（忽略）");
        }
    }

    /// <summary>每秒一次：聚合 stock_sync_task 状态计数 + 当前 running task 的 L2 步级 + fetch halt 态。</summary>
    private async Task PollLoopAsync(DateTimeOffset startedAt, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var snapshot = await BuildSnapshotAsync(startedAt, ct);
                    _progress.Emit(snapshot);
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    // DB 暂时不可达 / PG 查询失败：poller 不死，下一秒再试；engine 继续跑
                    _logger.LogDebug(ex, "progress poll 失败，下一秒重试");
                }

                await Task.Delay(TimeSpan.FromSeconds(1), _time, ct);
            }
        }
        catch (OperationCanceledException) { /* 正常 */ }
    }

    private async Task<SyncProgress> BuildSnapshotAsync(DateTimeOffset startedAt, CancellationToken ct)
    {
        await using var scope = _root.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();

        var grouped = await db.StockSyncTasks.AsNoTracking()
            .GroupBy(t => t.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToListAsync(ct);

        int Count(string s) => grouped.FirstOrDefault(x => x.Status == s)?.Count ?? 0;
        var pending = Count("pending");
        var partial = Count("partial");
        var done = Count("done");
        var failed = Count("failed");
        var total = pending + partial + done + failed + Count("running");

        // L2：当前 running task 的 datasets_done
        string? currentCode = null;
        string? currentKind = null;
        IReadOnlyList<string>? currentSteps = null;
        var running = await db.StockSyncTasks.AsNoTracking()
            .Where(t => t.Status == "running")
            .OrderBy(t => t.UpdatedAt)
            .Select(t => new { t.Code, t.Kind, t.DatasetsDone })
            .FirstOrDefaultAsync(ct);
        if (running is not null)
        {
            currentCode = running.Code;
            currentKind = running.Kind;
            currentSteps = running.DatasetsDone;
        }

        // fetch halt（best-effort；fetch 不可达不阻断进度快照）
        FetchHaltedInfo? halted = null;
        try
        {
            var st = await _control.GetStatusAsync(ct);
            if (st?.IsHalted == true) halted = st.Halted;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "fetch /status 探测失败（忽略，poller 不死）");
        }

        // ETA：按 done 速率推算（粗估：剩余 = total - done；速率 = done / elapsed）
        var elapsed = _time.GetUtcNow() - startedAt;
        TimeSpan? eta = null;
        if (done > 0 && elapsed > TimeSpan.Zero && total > done)
        {
            var ratePerSec = done / elapsed.TotalSeconds;
            if (ratePerSec > 0)
            {
                var remainingSec = (total - done) / ratePerSec;
                eta = TimeSpan.FromSeconds(remainingSec);
            }
        }

        return new SyncProgress(
            Done: done,
            Total: total,
            Pending: pending,
            Partial: partial,
            Failed: failed,
            CurrentCode: currentCode,
            CurrentKind: currentKind,
            CurrentStepsDone: currentSteps,
            Elapsed: elapsed,
            EstimatedRemaining: eta,
            Halted: halted);
    }
}