using Microsoft.Extensions.Logging.Abstractions;
using StockData.Mcp.Fetching;
using StockData.SyncCli;

namespace StockData.SyncCli.Tests;

/// <summary>
/// HaltMonitor（CLI 包装）的薄壁测试：
/// RunAsync 必须原样委托给被包装的 FetchHaltMonitor.RunAsync。
/// 此处通过"传一个 pre-cancelled token + 不会拉起真实 fetch 的 stub"验证：
///   - 立即返回，不抛
///   - 不阻塞（&lt; 1s 完成）—— 确认 cancellation 透传，未被 wrapper 吞掉
///
/// 不验证更复杂行为：wrapper 是 1 行委托（_halt.RunAsync(ct)），反射测试脆弱；
/// 冷却闸逻辑（ShouldRestart / 调度）由 Mcp.Tests/StockData.Mcp.Tests.FetchHaltMonitorTests 覆盖。
/// </summary>
public class HaltMonitorTests
{
    private sealed class StubFetchControl : IFetchControl
    {
        public Task<FetchStatusResponse?> GetStatusAsync(CancellationToken ct = default) =>
            Task.FromResult<FetchStatusResponse?>(null);
        public Task<FetchRestartResponse?> RestartAsync(CancellationToken ct = default) =>
            Task.FromResult<FetchRestartResponse?>(null);
    }

    [Fact]
    public async Task RunAsync_已取消令牌_立即返回_不抛()
    {
        var inner = new FetchHaltMonitor(
            new StubFetchControl(),
            new FetchHaltMonitorOptions { PollSeconds = 60, RestartCooldownSeconds = 600 },
            TimeProvider.System,
            NullLogger<FetchHaltMonitor>.Instance);

        var wrapper = new HaltMonitor(inner);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await wrapper.RunAsync(cts.Token);
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1),
            $"wrapper.RunAsync 在 pre-cancelled token 下应瞬时返回，实际耗时 {sw.Elapsed}");
    }
}
