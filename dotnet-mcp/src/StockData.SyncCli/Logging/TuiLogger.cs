using Microsoft.Extensions.Logging;

namespace StockData.SyncCli.Logging;

/// <summary>
/// TUI 模式专用的日志捕获：把所有 <see cref="ILogger"/> 输出塞进线程安全 ring buffer，
/// 供 <see cref="Ui.DashboardWindow"/> 的 log 面板渲染。<b>不写 <c>Console.Out</c></b>（那是
/// Terminal.Gui 的领地，二者抢同一个流会输出乱码 → 面板渲染错位）。
///
/// <para>
/// 设计要点：
/// <list type="bullet">
///   <item>容量 500 条，超出丢最旧（FIFO）；足够覆盖一个 drain 周期内的 warn/error + 关键 info。</item>
///   <item>提供 <see cref="OnLogged"/> 事件供 dashboard 实时订阅；订阅方需自己 throttle / marshal。</item>
///   <item>category-aware：默认全收，由 <c>ILoggerFactory</c> 的 filter 决定哪些进得来
///         （Program.cs 配 EF/Http 等噪声 → Warning+）。</item>
/// </list>
/// </para>
/// </summary>
public sealed class TuiLoggerProvider : ILoggerProvider
{
    private readonly int _capacity;
    private readonly object _lock = new();
    private readonly LinkedList<LogEntry> _entries = new();

    /// <summary>新日志入列时触发（在写入线程上同步触发；订阅方必须自己 marshal 到 UI 线程）。</summary>
    public event Action<LogEntry>? OnLogged;

    /// <summary>构造一个 capacity 条的 ring buffer。</summary>
    /// <param name="capacity">最大保留条数；超出按 FIFO 丢最旧。默认 500。</param>
    public TuiLoggerProvider(int capacity = 500) => _capacity = capacity;

    public ILogger CreateLogger(string categoryName) => new TuiLogger(this, categoryName);

    internal void Write(string category, LogLevel level, string message, Exception? exception)
    {
        var entry = new LogEntry(DateTimeOffset.Now, category, level, message, exception);
        lock (_lock)
        {
            _entries.AddLast(entry);
            while (_entries.Count > _capacity) _entries.RemoveFirst();
        }
        OnLogged?.Invoke(entry);
    }

    /// <summary>取最近 count 条快照（线程安全拷贝；dashboard 轮询用）。</summary>
    public IReadOnlyList<LogEntry> Snapshot(int count)
    {
        lock (_lock)
        {
            if (_entries.Count <= count) return _entries.ToList();
            return _entries.Skip(_entries.Count - count).ToList();
        }
    }

    /// <summary>当前 buffer 里的条数（线程安全；仅供测试 / 调试用）。</summary>
    public int Count
    {
        get { lock (_lock) return _entries.Count; }
    }

    public void Dispose()
    {
        // 订阅方随 provider 一起 GC；buffer 自身无需释放。
        OnLogged = null;
    }
}

/// <summary>单条日志：时间、分类、级别、消息、异常（可空）。</summary>
/// <param name="Timestamp">UTC 偏移（落地时间）。</param>
/// <param name="Category">日志来源 category（通常 = 类全名）。</param>
/// <param name="Level">日志级别。</param>
/// <param name="Message">格式化后的消息。</param>
/// <param name="Exception">关联异常（可空）。</param>
public sealed record LogEntry(
    DateTimeOffset Timestamp,
    string Category,
    LogLevel Level,
    string Message,
    Exception? Exception)
{
    /// <summary>单行渲染：<c>[HH:mm:ss] [LEVEL] category: message</c>。异常额外追加 <c>— Type: Msg</c>（仅类型+Message，避免堆栈刷屏）。</summary>
    public string Format() =>
        Exception is null
            ? $"[{Timestamp.ToLocalTime():HH:mm:ss}] [{Level}] {ShortCategory(Category)}: {Message}"
            : $"[{Timestamp.ToLocalTime():HH:mm:ss}] [{Level}] {ShortCategory(Category)}: {Message} \u2014 {Exception.GetType().Name}: {Exception.Message}";

    /// <summary>把 <c>StockData.SyncCli.Foo.Bar</c> 缩成 <c>SyncCli.Foo.Bar</c>，让面板更紧凑。</summary>
    private static string ShortCategory(string c) =>
        c.StartsWith("StockData.", StringComparison.Ordinal) ? c["StockData.".Length..] : c;
}

internal sealed class TuiLogger(TuiLoggerProvider provider, string category) : ILogger
{
    public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    // 过滤交给 LoggerFactory 的 AddFilter；这里永远 IsEnabled=true，
    // 否则 StockData.* 的 warn 也可能被 factory 提前丢。
    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => provider.Write(category, level, formatter(state, exception), exception);
}

internal sealed class NullScope : IDisposable
{
    public static readonly NullScope Instance = new();
    public void Dispose() { }
}
