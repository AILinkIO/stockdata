using System.Text.Json;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>
/// fetch 暂停监视的冷却闸逻辑（FetchHaltMonitor.ShouldRestart）：
/// 仅在 worker=halted 且暂停已持续 ≥ 冷却时长时才自动 /restart。
/// 顺带校验 /status、/restart 的 snake_case JSON 反序列化契约。
/// </summary>
public class FetchHaltMonitorTests
{
    private const int Cooldown = 600;
    private static FetchStatusResponse Halted(long since) =>
        new("stockdata-fetch", "halted", new FetchHaltedInfo("黑名单用户 (code: 10001011)", since));

    [Fact]
    public void 运行态_不恢复()
    {
        var running = new FetchStatusResponse("stockdata-fetch", "running", null);
        Assert.False(FetchHaltMonitor.ShouldRestart(running, nowUnix: 10_000, Cooldown));
    }

    [Fact]
    public void 空状态_不恢复()
    {
        Assert.False(FetchHaltMonitor.ShouldRestart(null, nowUnix: 10_000, Cooldown));
    }

    [Fact]
    public void 暂停未满冷却_不恢复()
    {
        var since = 10_000L;
        Assert.False(FetchHaltMonitor.ShouldRestart(Halted(since), since + Cooldown - 1, Cooldown));
    }

    [Fact]
    public void 暂停满冷却_恢复()
    {
        var since = 10_000L;
        Assert.True(FetchHaltMonitor.ShouldRestart(Halted(since), since + Cooldown, Cooldown));
        Assert.True(FetchHaltMonitor.ShouldRestart(Halted(since), since + Cooldown + 5_000, Cooldown));
    }

    [Fact]
    public void status_json_反序列化契约()
    {
        const string json = """
            {"name":"stockdata-fetch","worker":"halted",
             "halted":{"reason":"Baostock 登录被拉黑: 黑名单用户 (code: 10001011)","since":1782377628}}
            """;
        var s = JsonSerializer.Deserialize<FetchStatusResponse>(json, HttpFetchClient.Json);
        Assert.NotNull(s);
        Assert.True(s!.IsHalted);
        Assert.Equal(1782377628, s.Halted!.Since);
        Assert.Contains("10001011", s.Halted.Reason);
    }

    [Fact]
    public void restart_json_反序列化契约()
    {
        const string json = """
            {"status":"ok","worker":"running","was_halted":true,
             "previous":{"reason":"x","since":1782377628}}
            """;
        var r = JsonSerializer.Deserialize<FetchRestartResponse>(json, HttpFetchClient.Json);
        Assert.NotNull(r);
        Assert.True(r!.WasHalted);
        Assert.Equal("running", r.Worker);
    }
}
