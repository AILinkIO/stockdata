using System.Text.Json;

namespace StockData.Mcp.StockDataClient;

/// <summary>
/// server REST API（:8080）的 typed client。
/// 透传约定：返回原样 JSON 文本（不经 double 反序列化，保持数值精度）；
/// HTTP 错误映射为 "Error: ..." 字符串（沿用旧 MCP 工具约定，便于 LLM 理解）。
/// 504（读穿透超时）语义是"稍后重试必命中"，由 resilience 管道自动重试。
/// </summary>
public sealed class StockDataApiClient(HttpClient http)
{
    public async Task<string> GetAsync(string path, Dictionary<string, string?>? query = null,
        CancellationToken ct = default)
    {
        var url = path;
        if (query is { Count: > 0 })
        {
            var qs = string.Join("&", query
                .Where(kv => kv.Value is not null)
                .Select(kv => $"{Uri.EscapeDataString(kv.Key)}={Uri.EscapeDataString(kv.Value!)}"));
            if (qs.Length > 0) url = $"{path}?{qs}";
        }

        HttpResponseMessage resp;
        try
        {
            resp = await http.GetAsync(url, ct);
        }
        catch (Exception e) when (e is HttpRequestException or TaskCanceledException)
        {
            return $"Error: 无法连接数据服务（{http.BaseAddress}）: {e.Message}";
        }

        var body = await resp.Content.ReadAsStringAsync(ct);
        if (resp.IsSuccessStatusCode) return body;

        // 错误响应取 detail 字段，取不到则原文
        try
        {
            using var doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("detail", out var detail))
                return $"Error: {detail.GetString()}";
        }
        catch (JsonException) { /* 非 JSON 错误体，用原文 */ }
        return $"Error: HTTP {(int)resp.StatusCode}: {body}";
    }
}
