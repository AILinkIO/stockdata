namespace StockData.SyncCli;

/// <summary>
/// 终端运行模式：<see cref="Tui"/> = 交互 Terminal.Gui 面板（Phase 2）；<see cref="Log"/> =
/// 结构化日志（docker detached 或 pipe 到 cat 时）。由 <see cref="ConsoleModeDetector.Detect"/>
/// 基于重定向标志自动选：pipe / 重定向 = Log，原始 TTY = Tui（Phase 1 暂未实现 TUI，会 fallback 到 Log）。
/// </summary>
public enum ConsoleMode
{
    /// <summary>交互终端：Phase 2 接入 Terminal.Gui Dashboard。</summary>
    Tui,
    /// <summary>headless：输出结构化日志到 stdout，供 docker logs / 管道消费。</summary>
    Log
}

/// <summary>
/// TTY 检测：<c>IsOutputRedirected</c> 或 <c>IsInputRedirected</c> 任一为 true → Log；
/// 否则视为真实交互终端 → Tui。
/// </summary>
public static class ConsoleModeDetector
{
    public static ConsoleMode Detect() =>
        Console.IsOutputRedirected || Console.IsInputRedirected ? ConsoleMode.Log : ConsoleMode.Tui;
}