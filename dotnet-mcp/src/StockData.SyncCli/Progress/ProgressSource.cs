namespace StockData.SyncCli.Progress;

/// <summary>
/// 线程安全实现：订阅/触发都在 try/finally 内原子交换 event 字段，
/// 避免订阅者在事件触发瞬间取消订阅导致迭代器爆炸。
/// </summary>
public sealed class ProgressSource : IProgressSource
{
    private Action<SyncProgress>? _updated;

    public event Action<SyncProgress>? Updated
    {
        add => _updated += value;
        remove => _updated -= value;
    }

    public void Emit(SyncProgress progress)
    {
        var handlers = _updated;   // 单读快照：触发时订阅者若有增删不影响本轮
        if (handlers is null) return;
        try
        {
            handlers(progress);
        }
        catch (Exception ex)
        {
            // sink 抛异常不能把发事件一侧（SyncEngine poller）打挂；落到控制台即可
            Console.Error.WriteLine($"[ProgressSource] subscriber threw: {ex}");
        }
    }
}