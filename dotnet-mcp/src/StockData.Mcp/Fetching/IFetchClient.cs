namespace StockData.Mcp.Fetching;

/// <summary>
/// 抓取客户端：提交一段区间抓取并等待结果（submit + poll，封装在实现里）。
/// 抽象出来便于编排逻辑（KlineService）用 fake 单测，无需真打 Python/baostock。
/// </summary>
public interface IFetchClient
{
    /// <summary>提交抓取并阻塞到 done，返回原始 payload。超时/失败抛异常（由调用方上抛 504 等价）。</summary>
    Task<FetchPayload> FetchAsync(FetchRequest request, CancellationToken ct = default);
}

/// <summary>抓取超时（读穿透等待耗尽）——映射为 504 等价，dotnet resilience 重试。</summary>
public sealed class FetchTimeoutException(string message) : Exception(message);

/// <summary>抓取失败（Python 侧退避耗尽标 failed）。</summary>
public sealed class FetchFailedException(string message) : Exception(message);
