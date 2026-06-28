using StockData.Mcp.Fetching;

namespace StockData.SyncCli.Progress;

/// <summary>
/// 同步进度快照（不可变 record）：TUI 渲染 / 日志 sink / Phase 4 测试都基于它。
/// 字段分别对应 L1（票级 done/total/partial/failed/pending）、L2（当前票 + 已完成步）、
/// L3 占位（当前留空，后续在 stock_sync_task 里加切片后填）。
///
/// 速率/ETA 由订阅方按本快照时间戳自行计算（避免在本 record 内引入 Timer/锁）。
/// </summary>
public sealed record SyncProgress(
    int Done,
    int Total,
    int Pending,
    int Partial,
    int Failed,
    string? CurrentCode,
    string? CurrentKind,
    IReadOnlyList<string>? CurrentStepsDone,
    TimeSpan Elapsed,
    TimeSpan? EstimatedRemaining,
    FetchHaltedInfo? Halted)
{
    /// <summary>空快照：启动首拍或无任务时用，所有数值字段为 0。</summary>
    public static SyncProgress Empty { get; } = new(
        Done: 0, Total: 0, Pending: 0, Partial: 0, Failed: 0,
        CurrentCode: null, CurrentKind: null, CurrentStepsDone: null,
        Elapsed: TimeSpan.Zero, EstimatedRemaining: null, Halted: null);
}