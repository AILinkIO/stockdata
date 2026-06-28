namespace StockData.SyncCli.Progress;

/// <summary>
/// 进度事件源：SyncEngine 发、UI/Log sink 订阅。event 而非 IObservable 是为了：
///   1) 与现有 ILogger&lt;T&gt; / 框架风格统一（C# event 是最熟悉的形态）；
///   2) 同步触发 + 轻量：单进程订阅者数 ≤2，避免引入响应式框架。
/// Emit 节流由调用方（poller）负责，订阅方假设事件频率 ≤ 1Hz。
/// </summary>
public interface IProgressSource
{
    event Action<SyncProgress>? Updated;

    /// <summary>主动推送一次快照（启动首拍 / 关闭末拍等场景）。</summary>
    void Emit(SyncProgress progress);
}