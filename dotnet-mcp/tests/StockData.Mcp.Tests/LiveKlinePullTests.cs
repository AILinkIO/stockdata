using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>
/// 真实拉取（live）：整条 dotnet 链路打真 fetch_service → baostock → 落盘真 PG。
/// 默认跳过——需 STOCKDATA_LIVE=1 + STOCKDATA_PG_DSN（目标库）+ STOCKDATA_FETCH_BASE（默认 :8090）。
/// ⚠️ 会真实打 baostock（首次触达 = 全史回填，多段查询，走 fetch_service 限流）。
/// </summary>
[Trait("Category", "Live")]
public class LiveKlinePullTests
{
    [Fact]
    public async Task 真实拉取_sh600000_日线_首次回填落盘()
    {
        if (Environment.GetEnvironmentVariable("STOCKDATA_LIVE") != "1") return;
        var pgDsn = Environment.GetEnvironmentVariable("STOCKDATA_PG_DSN");
        if (string.IsNullOrWhiteSpace(pgDsn)) return;

        var opts = new DbContextOptionsBuilder<StockDataDbContext>()
            .UseNpgsql(StockDataDbContextFactory.ToNpgsql(pgDsn)).Options;
        var fetchUrl = Environment.GetEnvironmentVariable("STOCKDATA_FETCH_BASE") ?? "http://127.0.0.1:8090";

        using var http = new HttpClient { BaseAddress = new Uri(fetchUrl), Timeout = Timeout.InfiniteTimeSpan };
        var fetch = new HttpFetchClient(http, new FetchClientOptions { WaitTimeoutSeconds = 180 }, TimeProvider.System);

        await using var db = new StockDataDbContext(opts);
        var svc = new KlineService(new EfWatermarkStore(db), fetch, new KlineWriter(db));

        // 首次触达 → 从 1990-12-19 全史回填到请求尾，多段切片，逐段落库推进水位
        await svc.EnsureRangeAsync("sh.600000", "k_d", new DateOnly(2026, 6, 1), new DateOnly(2026, 6, 20), DateTimeOffset.UtcNow);

        var count = await db.Klines.CountAsync(k => k.Code == "sh.600000" && k.Frequency == "d");
        Assert.True(count > 1000, $"日线行数={count}，应为全史回填的数千行");

        var wm = await db.DataWatermarks.AsNoTracking()
            .FirstAsync(w => w.Code == "sh.600000" && w.DataType == "k_d");
        Assert.Equal(new DateOnly(1990, 12, 19), wm.FirstDate);   // 回填起点
        Assert.True(wm.LastDate >= new DateOnly(2026, 6, 19), $"水位 last={wm.LastDate}");
    }
}
