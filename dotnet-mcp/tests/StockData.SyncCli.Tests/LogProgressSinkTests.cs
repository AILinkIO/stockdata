using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StockData.Mcp.Fetching;
using StockData.SyncCli.Logging;
using StockData.SyncCli.Progress;

namespace StockData.SyncCli.Tests;

/// <summary>
/// LogProgressSink 的节流 + 关键事件免节流：
///   - 首拍快照立即输出（_lastDone 初值 -1 触发 firstSnapshot 分支）
///   - 5s 节流窗口内：相同 Done + 相同 halted 状态 → 抑制
///   - 5s 节流窗口内：Done 递增 → 立即输出（任务完成事件）
///   - 5s 节流窗口内：halted 状态翻转（null↔non-null）→ 立即输出
///   - 节流窗口外（间隔 > 5s）→ 总是输出
///   - Dispose 后取消订阅，Emit 不再触发 sink
/// </summary>
public class LogProgressSinkTests
{
    private sealed class FakeTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;
        public FakeTimeProvider(DateTimeOffset start) => _now = start;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan t) => _now += t;
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public List<string> Messages { get; } = new();
        public ILogger CreateLogger(string categoryName) => new CapturingLogger(Messages);
        public void Dispose() { }
        private sealed class CapturingLogger(List<string> sink) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
                Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Information) sink.Add(formatter(state, exception));
            }
        }
    }

    private static SyncProgress Snap(int done = 1, int total = 100, FetchHaltedInfo? halted = null) =>
        SyncProgress.Empty with { Done = done, Total = total, Halted = halted };

    private static FetchHaltedInfo Halted(long since) =>
        new("黑名单用户 (code: 10001011)", since);

    [Fact]
    public void 首次快照_立即输出()
    {
        var src = new ProgressSource();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var provider = new CapturingLoggerProvider();
        using var lf = LoggerFactory.Create(b => b.AddProvider(provider));

        using var sink = new LogProgressSink(src,
            lf.CreateLogger<LogProgressSink>(), time);

        src.Emit(Snap(done: 1));

        Assert.Single(provider.Messages);
    }

    [Fact]
    public void 节流窗口内_仅当done递增才输出()
    {
        var src = new ProgressSource();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var provider = new CapturingLoggerProvider();
        using var lf = LoggerFactory.Create(b => b.AddProvider(provider));

        using var sink = new LogProgressSink(src,
            lf.CreateLogger<LogProgressSink>(), time);

        src.Emit(Snap(done: 1));
        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 1));

        Assert.Single(provider.Messages);
    }

    [Fact]
    public void 节流窗口内_done递增_立即输出()
    {
        var src = new ProgressSource();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var provider = new CapturingLoggerProvider();
        using var lf = LoggerFactory.Create(b => b.AddProvider(provider));

        using var sink = new LogProgressSink(src,
            lf.CreateLogger<LogProgressSink>(), time);

        src.Emit(Snap(done: 1));
        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 2));
        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 3));

        Assert.Equal(3, provider.Messages.Count);
    }

    [Fact]
    public void 节流窗口内_halted状态翻转_立即输出()
    {
        var src = new ProgressSource();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var provider = new CapturingLoggerProvider();
        using var lf = LoggerFactory.Create(b => b.AddProvider(provider));

        using var sink = new LogProgressSink(src,
            lf.CreateLogger<LogProgressSink>(), time);

        src.Emit(Snap(done: 1, halted: null));
        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 1, halted: null));
        Assert.Single(provider.Messages);

        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 1, halted: Halted(1_000)));
        Assert.Equal(2, provider.Messages.Count);

        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 1, halted: Halted(1_000)));
        Assert.Equal(2, provider.Messages.Count);

        time.Advance(TimeSpan.FromSeconds(1));
        src.Emit(Snap(done: 1, halted: null));
        Assert.Equal(3, provider.Messages.Count);
    }

    [Fact]
    public void 节流窗口外_总是输出()
    {
        var src = new ProgressSource();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var provider = new CapturingLoggerProvider();
        using var lf = LoggerFactory.Create(b => b.AddProvider(provider));

        using var sink = new LogProgressSink(src,
            lf.CreateLogger<LogProgressSink>(), time);

        src.Emit(Snap(done: 1));
        time.Advance(TimeSpan.FromSeconds(6));
        src.Emit(Snap(done: 1));
        time.Advance(TimeSpan.FromSeconds(6));
        src.Emit(Snap(done: 1));

        Assert.Equal(3, provider.Messages.Count);
    }

    [Fact]
    public void Dispose_取消订阅()
    {
        var src = new ProgressSource();
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var provider = new CapturingLoggerProvider();
        using var lf = LoggerFactory.Create(b => b.AddProvider(provider));

        var sink = new LogProgressSink(src,
            lf.CreateLogger<LogProgressSink>(), time);
        sink.Dispose();

        src.Emit(Snap(done: 1));

        Assert.Empty(provider.Messages);
    }
}
