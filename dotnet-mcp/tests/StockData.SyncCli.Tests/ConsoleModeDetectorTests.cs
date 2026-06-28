namespace StockData.SyncCli.Tests;

/// <summary>
/// ConsoleModeDetector 的测试占位。
///
/// Console.IsOutputRedirected / IsInputRedirected 是绑定真实 stdout/stdin 的静态属性，
/// 无法在单元测试里安全重定向（多个测试并行运行会相互污染，且
/// xunit 并行 collection 隔离也不能保证 stdout 是线程局部的）。
///
/// 该逻辑改由 Phase 1 的烟雾测试覆盖：
///   - <c>dotnet run -- drain --watch | cat</c> → Log 模式（pipe 重定向）
///   - <c>dotnet run -- drain --watch</c>（真实 TTY）→ Tui 模式（Phase 2 落地后）
///
/// v2 重构候选：抽出 IConsoleState 接口（暴露 IsOutputRedirected / IsInputRedirected）
/// 便于注入；改动小、收益集中在测试性。
/// </summary>
public class ConsoleModeDetectorTests
{
    [Fact]
    public void 占位_未来加入可注入_console_state_抽象后补单测()
    {
        Assert.True(true);
    }
}
