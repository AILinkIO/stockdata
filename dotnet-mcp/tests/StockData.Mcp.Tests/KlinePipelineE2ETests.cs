using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>
/// 隔离空库端到端脚手架：真 PG（独立空库）+ fake fetch（**不打 baostock**），
/// 验证 coverage → 切片 → 落盘（EF/ON CONFLICT 同事务）→ 直读 → 再判新鲜 整条链路。
///
/// 默认跳过——需设 <c>STOCKDATA_E2E_PG_DSN</c> 指向一个**独立空库**（非现网 stockdata）。
/// 测试会 EnsureDeleted + Migrate 重建该库；内置护栏拒绝库名为 stockdata，杜绝误伤现网。
/// 用法见 <c>scripts/e2e-kline.sh</c>。
/// </summary>
[Trait("Category", "E2E")]
public class KlinePipelineE2ETests
{
    /// <summary>echo fetch：为请求区间的起止日各回一行（baostock 形状），不触网络。</summary>
    private sealed class EchoFetch : IFetchClient
    {
        public List<FetchRequest> Calls { get; } = new();

        public Task<FetchPayload> FetchAsync(FetchRequest r, CancellationToken ct = default)
        {
            Calls.Add(r);
            var dates = new[] { r.StartDate!.Value, r.EndDate!.Value }.Distinct().OrderBy(d => d);
            var rows = dates
                .Select(d => (IReadOnlyList<string?>)new string?[] { d.ToString("yyyy-MM-dd"), "10.00", "10.50", "1000" })
                .ToList();
            return Task.FromResult(new FetchPayload(new[] { "date", "open", "close", "volume" }, rows));
        }
    }

    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    [Fact]
    public async Task 隔离空库_全链路_落盘水位推进_重判新鲜()
    {
        var dsn = Environment.GetEnvironmentVariable("STOCKDATA_E2E_PG_DSN");
        if (string.IsNullOrWhiteSpace(dsn)) return;  // 未提供隔离库 → 跳过（不连库）

        var conn = StockDataDbContextFactory.ToNpgsql(dsn);
        GuardNotProduction(conn);
        var opts = new DbContextOptionsBuilder<StockDataDbContext>().UseNpgsql(conn).Options;

        // 1. 重建隔离空库 schema（清表 + Migrate；库由 scripts/e2e-kline.sh 预建，
        //    不用 EnsureDeleted 以免需要 DROP/CREATE DATABASE 权限）
        await using (var db = new StockDataDbContext(opts))
        {
            await db.Database.ExecuteSqlRawAsync(
                "DROP TABLE IF EXISTS kline, data_watermark, \"__EFMigrationsHistory\" CASCADE;");
            await db.Database.MigrateAsync();

            // 预置水位：覆盖到 2024-01-01，使请求 [01-02,01-03] 形成单段尾部缺口（避开 1990 全史回填）
            db.DataWatermarks.Add(new DataWatermark
            {
                Code = "sh.600000", DataType = "k_d",
                FirstDate = D(2020, 1, 1), LastDate = D(2024, 1, 1), LastFetchedAt = DateTimeOffset.UtcNow,
            });
            await db.SaveChangesAsync();
        }

        var fetch = new EchoFetch();
        var now = DateTimeOffset.UtcNow;  // 与 DB now() 对齐（数据在 2024，相对 2026 已定型）

        // 2. 首次 EnsureRange：判出尾部缺口 → fetch → 落盘
        await using (var db = new StockDataDbContext(opts))
        {
            var svc = new KlineService(new EfWatermarkStore(db), fetch, new KlineWriter(db));
            await svc.EnsureRangeAsync("sh.600000", "k_d", D(2024, 1, 2), D(2024, 1, 3), now);
        }

        Assert.Single(fetch.Calls);
        Assert.Equal(D(2024, 1, 2), fetch.Calls[0].StartDate);
        Assert.Equal(D(2024, 1, 3), fetch.Calls[0].EndDate);

        // 3. 直读 PG 验证落盘与水位推进
        await using (var db = new StockDataDbContext(opts))
        {
            var rows = await db.Klines.AsNoTracking()
                .Where(k => k.Code == "sh.600000" && k.Frequency == "d")
                .OrderBy(k => k.TradeDate).ToListAsync();
            Assert.Equal(2, rows.Count);
            Assert.Equal(D(2024, 1, 2), rows[0].TradeDate);
            Assert.Equal(10.50m, rows[1].Close);          // 字符串 → decimal 精确落库
            Assert.Equal(1000L, rows[0].Volume);

            var wm = await db.DataWatermarks.AsNoTracking()
                .FirstAsync(w => w.Code == "sh.600000" && w.DataType == "k_d");
            Assert.Equal(D(2024, 1, 3), wm.LastDate);     // claimable_last 推进到实际数据末
            Assert.Equal(D(2020, 1, 1), wm.FirstDate);    // LEAST 保留更早
        }

        // 4. 再次 EnsureRange 同范围 → coverage 判新鲜 → 不再 fetch
        fetch.Calls.Clear();
        await using (var db = new StockDataDbContext(opts))
        {
            var svc = new KlineService(new EfWatermarkStore(db), fetch, new KlineWriter(db));
            await svc.EnsureRangeAsync("sh.600000", "k_d", D(2024, 1, 2), D(2024, 1, 3), now);
        }
        Assert.Empty(fetch.Calls);
    }

    /// <summary>护栏：库名为 stockdata（现网）时拒绝运行，杜绝误伤。</summary>
    private static void GuardNotProduction(string conn)
    {
        var db = conn.Split(';')
            .Select(p => p.Trim())
            .FirstOrDefault(p => p.StartsWith("Database=", StringComparison.OrdinalIgnoreCase))?
            ["Database=".Length..];
        if (string.Equals(db, "stockdata", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "E2E 护栏：STOCKDATA_E2E_PG_DSN 指向了现网库 stockdata，拒绝运行。请用独立空库（如 stockdata_e2e）。");
    }
}
