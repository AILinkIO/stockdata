using StockData.SyncCli.Progress;

namespace StockData.SyncCli.Tests;

/// <summary>
/// ProgressSource 的线程安全 + 订阅者异常隔离：
///   - 无订阅者时 Emit 静默返回
///   - 多订阅者全部触发
///   - 单个订阅者抛异常不会把发事件方（SyncEngine poller）打挂；其他订阅者照常跑
///   - 订阅者在触发期间自我反订阅不应抛（snapshot 复制 + try/catch 兜底）
/// </summary>
public class ProgressSourceTests
{
    private static SyncProgress Sample(int done = 7) =>
        SyncProgress.Empty with { Done = done, Total = 100 };

    [Fact]
    public void Emit_无订阅者_不抛()
    {
        var src = new ProgressSource();

        var ex = Record.Exception(() => src.Emit(Sample()));
        Assert.Null(ex);
    }

    [Fact]
    public void Emit_有订阅者_订阅者被调用()
    {
        var src = new ProgressSource();
        SyncProgress? captured = null;
        src.Updated += p => captured = p;

        src.Emit(Sample(done: 42));

        Assert.NotNull(captured);
        Assert.Equal(42, captured!.Done);
        Assert.Equal(100, captured.Total);
    }

    [Fact]
    public void Emit_多订阅者_全部被调用()
    {
        var src = new ProgressSource();
        var seen = new System.Collections.Concurrent.ConcurrentBag<int>();

        src.Updated += p => seen.Add(p.Done);
        src.Updated += p => seen.Add(p.Done * 10);
        src.Updated += p => seen.Add(p.Done * 100);

        src.Emit(Sample(done: 5));

        Assert.Equal(3, seen.Count);
        Assert.Contains(5, seen);
        Assert.Contains(50, seen);
        Assert.Contains(500, seen);
    }

    [Fact]
    public void Emit_订阅者抛异常_不影响发布方()
    {
        var src = new ProgressSource();
        var beforeThrowing = 0;

        src.Updated += _ => Interlocked.Increment(ref beforeThrowing);
        src.Updated += _ => throw new InvalidOperationException("boom");

        var ex = Record.Exception(() => src.Emit(Sample()));
        Assert.Null(ex);

        Assert.Equal(1, beforeThrowing);
    }

    [Fact]
    public void 订阅_触发期间反订阅_不抛()
    {
        var src = new ProgressSource();
        var firedAfterUnsub = 0;

        Action<SyncProgress> selfRemoving = null!;
        selfRemoving = _ =>
        {
            src.Updated -= selfRemoving;
        };
        src.Updated += selfRemoving;

        src.Emit(Sample());

        src.Updated += _ => firedAfterUnsub++;

        var ex = Record.Exception(() => src.Emit(Sample()));
        Assert.Null(ex);
        Assert.Equal(1, firedAfterUnsub);
    }
}
