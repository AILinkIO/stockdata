namespace StockData.Mcp.Fetching;

/// <summary>
/// 抓取优先级的环境标记（AsyncLocal）：避免把 priority 穿透进每个 EnsureXxxAsync 签名。
/// MCP 交互读路径用 <see cref="High"/> 包住其抓取 → HttpFetchClient 据此给 fetch 标 high 插队；
/// 后台 Drainer 不设 → 默认 low。仅影响 Python fetch 队列的取出顺序，不改抓取语义。
/// </summary>
public static class FetchPriority
{
    private static readonly AsyncLocal<bool> Flag = new();

    public static bool IsHigh => Flag.Value;

    /// <summary>在 using 作用域内把后续抓取标为高优先（嵌套安全：退出恢复原值）。</summary>
    public static IDisposable High()
    {
        var prev = Flag.Value;
        Flag.Value = true;
        return new Scope(prev);
    }

    private sealed class Scope(bool prev) : IDisposable
    {
        public void Dispose() => Flag.Value = prev;
    }
}
