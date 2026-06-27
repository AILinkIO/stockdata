using System.Threading.Channels;

namespace StockData.Mcp.Data;

/// <summary>
/// Drainer 唤醒信号：RegisterIfNew 创建新任务时 <see cref="Signal"/>，
/// SyncDrainer 空队列时 <see cref="WaitAsync"/> 替代固定 sleep。
///
/// 单例，unbounded 容量但 SingleReader（仅 Drainer 消费）。
/// 信号语义为"可能有活干了"的级别提示，不携带任务信息——Drainer 被唤醒后自行查库取任务。
/// 多次 Signal 在 Drainer 忙时会累积在 channel 里，Drainer 回到 WaitAsync 时一次性 drain。
/// </summary>
public sealed class SyncWakeUp
{
    private readonly Channel<object> _ch = Channel.CreateUnbounded<object>(
        new UnboundedChannelOptions { SingleReader = true });

    /// <summary>消费者（Drainer）：阻塞直到收到信号或被取消。返回前 drain 掉累积的信号。</summary>
    public async ValueTask WaitAsync(CancellationToken ct)
    {
        await _ch.Reader.WaitToReadAsync(ct);
        while (_ch.Reader.TryRead(out _)) { }
    }

    /// <summary>生产者（RegisterIfNew 等）：fire-and-forget 唤醒信号。TryWrite 在 unbounded channel 上永不失败。</summary>
    public void Signal() => _ch.Writer.TryWrite(new object());
}
