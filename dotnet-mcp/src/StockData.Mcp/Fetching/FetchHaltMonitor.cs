using System.Net.Http.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace StockData.Mcp.Fetching;

/// <summary>GET /status 响应：worker = running|halted；halted 仅暂停时有值。</summary>
public sealed record FetchStatusResponse(string Name, string Worker, FetchHaltedInfo? Halted)
{
    public bool IsHalted => string.Equals(Worker, "halted", StringComparison.OrdinalIgnoreCase);
}

/// <summary>暂停详情：Reason = 拉黑/接收错误消息，Since = 暂停起始 unix 秒。</summary>
public sealed record FetchHaltedInfo(string Reason, long Since);

/// <summary>POST /restart 响应。</summary>
public sealed record FetchRestartResponse(string Status, string Worker, bool WasHalted, FetchHaltedInfo? Previous);

/// <summary>
/// fetch 微服务控制面：感知暂停态（GET /status）、恢复抓取（POST /restart）。
/// 与 <see cref="IFetchClient"/> 分开，避免污染那条便于 fake 的取数路径。
/// </summary>
public interface IFetchControl
{
    Task<FetchStatusResponse?> GetStatusAsync(CancellationToken ct = default);
    Task<FetchRestartResponse?> RestartAsync(CancellationToken ct = default);
}

/// <summary>基于同一 FetchBase 的 typed HttpClient 实现。</summary>
public sealed class FetchControlClient(HttpClient http) : IFetchControl
{
    public Task<FetchStatusResponse?> GetStatusAsync(CancellationToken ct = default) =>
        http.GetFromJsonAsync<FetchStatusResponse>("/status", HttpFetchClient.Json, ct);

    public async Task<FetchRestartResponse?> RestartAsync(CancellationToken ct = default)
    {
        var resp = await http.PostAsync("/restart", content: null, ct);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<FetchRestartResponse>(HttpFetchClient.Json, ct);
    }
}

public sealed class FetchHaltMonitorOptions
{
    /// <summary>轮询 /status 的间隔（秒）。</summary>
    public int PollSeconds { get; set; } = 60;

    /// <summary>暂停持续多久才自动 /restart（秒）。≥ baostock 红线 5min，避免一暂停就重撞拉黑。</summary>
    public int RestartCooldownSeconds { get; set; } = 600;
}

/// <summary>
/// 后台监视 fetch 暂停态并自动恢复：周期轮询 GET /status，发现 halted 且已持续
/// ≥ <see cref="FetchHaltMonitorOptions.RestartCooldownSeconds"/> 时调 POST /restart。
///
/// 用「暂停已持续多久」（status.halted.since）当冷却闸：拉黑会持续很久、每次重登录都再撞、
/// 可能延长封禁，故不一暂停就 restart；冷却到点才尝试。若恢复后下个 job 又被拉黑，新的
/// since 重置 → 下次 restart 自然再隔一个冷却，restart 之间天然 ≥ 冷却间隔（尊重 >5min 红线）。
/// 仅管线开启（注册了 fetch 客户端）时挂载。
/// </summary>
public sealed class FetchHaltMonitor(
    IFetchControl control,
    FetchHaltMonitorOptions options,
    TimeProvider time,
    ILogger<FetchHaltMonitor> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "fetch 暂停监视已启动（轮询 {Poll}s / 冷却 {Cooldown}s）",
            options.PollSeconds, options.RestartCooldownSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // fetch 不可达/重启中等：忽略本轮，下个周期再试
                logger.LogDebug(ex, "fetch 状态轮询失败，忽略本轮");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(options.PollSeconds), time, stoppingToken);
            }
            catch (OperationCanceledException) { break; }
        }

        logger.LogInformation("fetch 暂停监视已停止");
    }

    private async Task TickAsync(CancellationToken ct)
    {
        var status = await control.GetStatusAsync(ct);
        if (status is null || !status.IsHalted)
            return;

        var now = time.GetUtcNow().ToUnixTimeSeconds();
        var haltedFor = HaltedSeconds(status, now);
        if (!ShouldRestart(status, now, options.RestartCooldownSeconds))
        {
            logger.LogInformation(
                "fetch 暂停中 {Held}s（冷却 {Cooldown}s 未到，暂不恢复）：{Reason}",
                haltedFor, options.RestartCooldownSeconds, status.Halted?.Reason);
            return;
        }

        logger.LogWarning(
            "fetch 已暂停 {Held}s，自动调用 /restart 恢复抓取：{Reason}",
            haltedFor, status.Halted?.Reason);
        var result = await control.RestartAsync(ct);
        logger.LogInformation("自动 /restart 完成（was_halted={Was}）", result?.WasHalted);
    }

    private static long HaltedSeconds(FetchStatusResponse status, long nowUnix) =>
        nowUnix - (status.Halted?.Since ?? nowUnix);

    /// <summary>暂停态且已持续满冷却才恢复——纯函数，便于单测冷却闸逻辑。</summary>
    internal static bool ShouldRestart(FetchStatusResponse? status, long nowUnix, int cooldownSeconds) =>
        status is { IsHalted: true } && HaltedSeconds(status, nowUnix) >= cooldownSeconds;
}
