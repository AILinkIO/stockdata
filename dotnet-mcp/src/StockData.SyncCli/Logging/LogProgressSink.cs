using Microsoft.Extensions.Logging;
using StockData.Mcp.Fetching;
using StockData.SyncCli.Progress;

namespace StockData.SyncCli.Logging;

/// <summary>
/// 日志模式 sink：订阅 <see cref="IProgressSource.Updated"/>，按节流（≥5s 一次）输出单行
/// 人可读进度；关键事件（halt 状态翻转、done 计数递增）无视节流立刻输出。
///
/// 构造即订阅：DI 容器化即可激活（Program.cs 在 Log 模式下 Resolve 此实例来强制订阅），
/// 不暴露 Unsubscribe——进程结束自然 GC。
/// </summary>
public sealed class LogProgressSink : IDisposable
{
    private readonly IProgressSource _source;
    private readonly ILogger<LogProgressSink> _logger;
    private readonly TimeProvider _time;
    private readonly TimeSpan _throttle;
    private DateTimeOffset _lastEmitAt = DateTimeOffset.MinValue;
    private int _lastDone = -1;
    private bool _lastHalted;     // false = 之前未 halted；首次出现 halted 立即打

    public LogProgressSink(IProgressSource source, ILogger<LogProgressSink> logger, TimeProvider time)
    {
        _source = source;
        _logger = logger;
        _time = time;
        _throttle = TimeSpan.FromSeconds(5);
        _source.Updated += OnUpdated;
    }

    private void OnUpdated(SyncProgress p)
    {
        var now = _time.GetUtcNow();
        var haltedNow = p.Halted is not null;

        // 关键事件：halted 状态翻转 OR done 递增（任务完成）
        var haltedChanged = haltedNow != _lastHalted;
        var doneAdvanced = p.Done != _lastDone;
        var firstSnapshot = _lastDone < 0;

        if (!firstSnapshot && !haltedChanged && !doneAdvanced)
        {
            // 节流窗口内静默
            if (now - _lastEmitAt < _throttle) return;
        }

        _lastEmitAt = now;
        _lastHalted = haltedNow;
        _lastDone = p.Done;

        _logger.LogInformation("{Line}", Format(p, now));
    }

    private static string Format(SyncProgress p, DateTimeOffset now)
    {
        var ts = now.ToLocalTime().ToString("HH:mm:ss");
        var pct = p.Total > 0 ? (p.Done * 100.0 / p.Total).ToString("F1") : "0.0";
        var current = p.CurrentCode is null
            ? "-"
            : $"{p.CurrentCode} ({p.CurrentKind ?? "-"},{StepsSummary(p.CurrentStepsDone)})";
        var halted = p.Halted is null ? "-" : $"{p.Halted.Reason} (since {p.Halted.Since})";
        var eta = p.EstimatedRemaining is null ? "-" : FormatSpan(p.EstimatedRemaining.Value);
        return $"[{ts}] progress: {p.Done}/{p.Total} codes ({pct}%) pending={p.Pending} partial={p.Partial} failed={p.Failed} | current: {current} | halted: {halted} | eta {eta} | elapsed {FormatSpan(p.Elapsed)}";
    }

    private static string StepsSummary(IReadOnlyList<string>? steps)
    {
        if (steps is null || steps.Count == 0) return "starting";
        return string.Join(",", steps);
    }

    private static string FormatSpan(TimeSpan t) =>
        t.TotalHours >= 1 ? $"{(int)t.TotalHours}h{t.Minutes}m" :
        t.TotalMinutes >= 1 ? $"{t.Minutes}m{t.Seconds}s" :
        $"{t.Seconds}s";

    public void Dispose() => _source.Updated -= OnUpdated;
}