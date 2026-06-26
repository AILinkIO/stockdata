using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// MCP 交互读的「定向 + 高优先 + 有界等待」抓取包装（TASK 本轮，方案 A）。
///
/// ServeFromPgOnly 模式下，读到缺/过期数据时调用：用 <see cref="FetchPriority.High"/> 让本次
/// 定向抓取在 Python fetch 队列里插队，并以 ReadFetchBudgetSeconds 作有界等待——预算内抓到
/// 则返回新鲜数据；超预算/失败则**吞掉异常**，调用方回退读 PG 现状（后台 Drainer 会补全）。
/// 真实请求被取消（ct）时不吞，照常抛出。
/// </summary>
internal static class ReadFetch
{
    /// <summary>
    /// 读路径统一抓取入口：pgOnly=true → 定向高优先有界抓取（超预算/失败吞掉，回退 PG）；
    /// false → 维持旧的无界读穿透（普通优先级）。调用方传入以本方法的 ct 重建的 ensure 闭包。
    /// </summary>
    public static Task EnsureAsync(IConfiguration config, bool pgOnly, CancellationToken ct, Func<CancellationToken, Task> ensure)
        => pgOnly ? TryAsync(config, ct, ensure) : ensure(ct);

    public static async Task TryAsync(IConfiguration config, CancellationToken ct, Func<CancellationToken, Task> ensure)
    {
        var budget = config.GetValue("StockData:ReadFetchBudgetSeconds", 30);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (budget > 0) cts.CancelAfter(TimeSpan.FromSeconds(budget));
        using (FetchPriority.High())
        {
            try
            {
                await ensure(cts.Token);
            }
            catch (Exception ex) when (
                !ct.IsCancellationRequested &&
                (ex is FetchTimeoutException or FetchFailedException
                 || (ex is OperationCanceledException && cts.IsCancellationRequested)))
            {
                // 预算内未抓完/抓失败：回退到 PG 现状，不让交互读挂死或报错；后台 Drainer 续抓
            }
        }
    }
}
