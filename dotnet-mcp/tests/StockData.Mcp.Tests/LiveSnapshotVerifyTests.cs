using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>快照三件套真实拉取验证（stock_list/index/industry）。需 STOCKDATA_LIVE=1 等环境变量。⚠️真打 baostock。</summary>
[Trait("Category", "Live")]
public class LiveSnapshotVerifyTests
{
    [Fact]
    public async Task 快照三件套真实拉取落盘()
    {
        if (Environment.GetEnvironmentVariable("STOCKDATA_LIVE") != "1") return;
        var pgDsn = Environment.GetEnvironmentVariable("STOCKDATA_PG_DSN");
        if (string.IsNullOrWhiteSpace(pgDsn)) return;

        var opts = new DbContextOptionsBuilder<StockDataDbContext>()
            .UseNpgsql(StockDataDbContextFactory.ToNpgsql(pgDsn)).Options;
        var fetchUrl = Environment.GetEnvironmentVariable("STOCKDATA_FETCH_BASE") ?? "http://127.0.0.1:8090";
        using var http = new HttpClient { BaseAddress = new Uri(fetchUrl), Timeout = Timeout.InfiniteTimeSpan };
        var fetch = new HttpFetchClient(http, new FetchClientOptions { WaitTimeoutSeconds = 180 }, TimeProvider.System);
        var now = DateTimeOffset.UtcNow;
        var snap = new DateOnly(2026, 6, 18);   // 确认的交易日

        await using var db = new StockDataDbContext(opts);
        var svc = new SnapshotService(fetch, new EfWatermarkStore(db));

        await svc.EnsureSnapshotAsync(new StockListIngest(db), snap, now);
        Assert.True(await db.StockListSnapshots.AnyAsync(x => x.SnapDate == snap), "stock_list 无数据");

        await svc.EnsureSnapshotAsync(new IndexConstituentIngest(db, "sz50"), snap, now);
        Assert.True(await db.IndexConstituents.AnyAsync(x => x.IndexCode == "sz50" && x.SnapDate == snap), "sz50 无数据");

        await svc.EnsureSnapshotAsync(new IndustryIngest(db), snap, now);
        Assert.True(await db.StockIndustries.AnyAsync(x => x.SnapDate == snap), "industry 无数据");
    }
}
