using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>
/// 部署整体验证：驱动各 dotnet 服务打真 fetch_service → baostock → 落真 PG，覆盖四种 coverage 模式。
/// 默认跳过——需 STOCKDATA_LIVE=1 + STOCKDATA_PG_DSN + STOCKDATA_FETCH_BASE。⚠️真实打 baostock。
/// </summary>
[Trait("Category", "Live")]
public class LiveDeployVerifyTests
{
    [Fact]
    public async Task 多类型真实拉取落盘()
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
        const string code = "sh.600000";

        // 1. trade_calendar（范围/全市场）
        await using (var db = new StockDataDbContext(opts))
        {
            await new TradeCalendarService(new EfWatermarkStore(db), fetch, new TradeCalendarWriter(db))
                .EnsureRangeAsync(new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 31), now);
            Assert.True(await db.TradeCalendars.CountAsync() > 0, "trade_calendar 无数据");
        }

        // 2. stock_basic（快照/per-code）
        await using (var db = new StockDataDbContext(opts))
        {
            await new SnapshotService(fetch, new EfWatermarkStore(db))
                .EnsureSnapshotAsync(new StockBasicIngest(db, code), Coverage.Today(now), now);
            Assert.True(await db.StockBasics.AnyAsync(x => x.Code == code), "stock_basic 无数据");
        }

        // 3. adjust_factor（恒全量；可能无除权，验证水位推进）
        await using (var db = new StockDataDbContext(opts))
        {
            await new AdjustFactorService(
                    new EfWatermarkStore(db), fetch, new AdjustFactorWriter(db),
                    new EfAdjustFactorSignalQuery(db))
                .EnsureFullAsync(code, new DateOnly(2026, 6, 10), now);
            Assert.True(await db.DataWatermarks.AnyAsync(w => w.Code == code && w.DataType == "adjust_factor"));
        }

        // 4. 宏观 deposit_rate（范围/全市场）
        await using (var db = new StockDataDbContext(opts))
        {
            await new MacroService(new EfWatermarkStore(db), fetch, new MacroWriter(db))
                .EnsureRangeAsync("deposit_rate", new DateOnly(2015, 1, 1), new DateOnly(2024, 12, 31), now);
            Assert.True(await db.DepositRates.CountAsync() > 0, "deposit_rate 无数据");
        }

        // 5. 财报季度（CheckQuarter；query_fina_quarter 6 类）
        await using (var db = new StockDataDbContext(opts))
        {
            await new FinancialQuarterService(db, fetch, new EfWatermarkStore(db), new FinancialWriter(db))
                .EnsureAsync(code, 2024, 1, now);
            Assert.True(await db.FinancialReports.AnyAsync(r => r.Code == code && r.StatDate == new DateOnly(2024, 3, 31)),
                "financial_report 无数据");
        }

        // 6. 分钟线（范围；近几日，复用 fetch_kline）
        await using (var db = new StockDataDbContext(opts))
        {
            await new KlineMinuteService(new EfWatermarkStore(db), fetch, new KlineMinuteWriter(db))
                .EnsureRangeAsync(code, 30, new DateOnly(2026, 6, 1), new DateOnly(2026, 6, 18), now);
            Assert.True(await db.KlineMinutes.AnyAsync(x => x.Code == code && x.Frequency == 30), "kline_minute 无数据");
        }
    }
}
