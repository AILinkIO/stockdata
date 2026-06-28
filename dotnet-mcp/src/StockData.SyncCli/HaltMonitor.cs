using StockData.Mcp.Fetching;

namespace StockData.SyncCli;

/// <summary>
/// 包装 MCP 的 <see cref="FetchHaltMonitor"/>：保持 RunAsync 公开签名与 SyncEngine 对称，
/// 留出未来加 CLI 特定 hook（如 halt 事件同时推送一条 LogProgressSink）的位置。
/// </summary>
public sealed class HaltMonitor
{
    private readonly FetchHaltMonitor _halt;

    public HaltMonitor(FetchHaltMonitor halt) => _halt = halt;

    public Task RunAsync(CancellationToken ct) => _halt.RunAsync(ct);
}