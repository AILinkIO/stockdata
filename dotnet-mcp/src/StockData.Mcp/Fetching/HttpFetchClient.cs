using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace StockData.Mcp.Fetching;

public sealed class FetchClientOptions
{
    /// <summary>读穿透等待超时（秒），沿用 Python fetch_wait_timeout 语义。</summary>
    public int WaitTimeoutSeconds { get; set; } = 120;

    /// <summary>轮询间隔（毫秒），沿用旧 _POLL_INTERVAL=0.5s。</summary>
    public int PollIntervalMs { get; set; } = 500;
}

/// <summary>
/// 调用 Python /fetch 微服务：POST 提交 → 轮询 GET 至 done（TASK D-A 异步 submit+poll）。
/// 超时抛 <see cref="FetchTimeoutException"/>（504 等价）；failed 抛 <see cref="FetchFailedException"/>。
/// HttpClient 的 BaseAddress 由 DI 配置（指向 Python 服务）。
/// </summary>
public sealed class HttpFetchClient(HttpClient http, FetchClientOptions options, TimeProvider time) : IFetchClient
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        // Python /fetch 返回 snake_case 键（job_id/fields/rows）：属性名与枚举均按 snake_case 映射
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower) },
    };

    public async Task<FetchPayload> FetchAsync(FetchRequest request, CancellationToken ct = default)
    {
        var submit = await http.PostAsJsonAsync("/fetch",
            new { type = request.Type, @params = request.ToParams() }, Json, ct);
        submit.EnsureSuccessStatusCode();
        var job = await submit.Content.ReadFromJsonAsync<FetchSubmitResponse>(Json, ct)
                  ?? throw new FetchFailedException("POST /fetch 返回空");

        var deadline = time.GetUtcNow().AddSeconds(options.WaitTimeoutSeconds);
        while (time.GetUtcNow() < deadline)
        {
            var resp = await http.GetFromJsonAsync<FetchJobResponse>($"/fetch/{job.JobId}", Json, ct)
                       ?? throw new FetchFailedException($"GET /fetch/{job.JobId} 返回空");

            switch (resp.Status)
            {
                case FetchStatus.Done:
                    return resp.Payload ?? FetchPayload.Empty;
                case FetchStatus.Failed:
                    throw new FetchFailedException($"fetch_kline {request.Code}: {resp.Error ?? "任务失败"}");
                default:
                    await Task.Delay(TimeSpan.FromMilliseconds(options.PollIntervalMs), time, ct);
                    break;
            }
        }
        throw new FetchTimeoutException($"fetch_kline {request.Code}: 等待抓取超时");
    }
}
