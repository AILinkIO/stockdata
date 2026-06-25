using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

public class AdjustCalcTests
{
    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    private static List<AdjustFactor> Factors() => new()
    {
        new() { DividOperateDate = D(2020, 1, 1), ForeAdjustFactor = 0.5m, BackAdjustFactor = 2.0m },
        new() { DividOperateDate = D(2022, 1, 1), ForeAdjustFactor = 0.8m, BackAdjustFactor = 1.5m },
    };

    private static Kline Bar(DateOnly d, decimal close) => new()
    {
        Code = "sh.600000", Frequency = "d", TradeDate = d,
        Open = close, High = close, Low = close, Close = close, Preclose = close,
    };

    [Fact]
    public void 前复权_fore_首事件前因子1_含事件当日()
    {
        var bars = new List<Kline>
        {
            Bar(D(2019, 12, 31), 10m),   // 首事件前 → 不变
            Bar(D(2020, 6, 1), 10m),     // 事件[2020] → fore 0.5 → 5
            Bar(D(2022, 1, 1), 10m),     // 事件当日(bisect_right 含) → fore 0.8 → 8
            Bar(D(2023, 1, 1), 10m),     // 最近事件 → 0.8 → 8
        };
        AdjustCalc.Apply(bars, Factors(), "2");

        Assert.Equal(10m, bars[0].Close);
        Assert.Equal(5.0m, bars[1].Close);
        Assert.Equal(8.0m, bars[2].Close);
        Assert.Equal(8.0m, bars[3].Close);
        Assert.Equal(5.0m, bars[1].Open);   // OHLC+preclose 同乘
        Assert.Equal(5.0m, bars[1].Preclose);
    }

    [Fact]
    public void 后复权_back()
    {
        var bars = new List<Kline> { Bar(D(2020, 6, 1), 10m), Bar(D(2022, 6, 1), 10m) };
        AdjustCalc.Apply(bars, Factors(), "1");
        Assert.Equal(20.0m, bars[0].Close);   // back 2.0
        Assert.Equal(15.0m, bars[1].Close);   // back 1.5
    }

    [Fact]
    public void 无因子_不变()
    {
        var bars = new List<Kline> { Bar(D(2020, 6, 1), 10m) };
        AdjustCalc.Apply(bars, new List<AdjustFactor>(), "2");
        Assert.Equal(10m, bars[0].Close);
    }

    [Fact]
    public void 解析_跳过无效日期与缺因子()
    {
        var payload = new FetchPayload(
            new[] { "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor" },
            new IReadOnlyList<string?>[]
            {
                new string?[] { "2020-01-01", "0.5", "2.0", "1.0" },
                new string?[] { "", "0.5", "2.0", "1.0" },        // 无日期 → 跳过
                new string?[] { "2022-01-01", "0.8", "1.5", null },
            });
        var rows = AdjustFactorParser.Parse(payload);
        Assert.Equal(2, rows.Count);
        Assert.Equal((D(2020, 1, 1), 0.5m, 2.0m, (decimal?)1.0m), rows[0]);
        Assert.Null(rows[1].Adjust);
    }
}

public class AdjustFactorServiceTests
{
    private static readonly DateTimeOffset NOW = new(2026, 6, 11, 12, 0, 0, TimeSpan.FromHours(8));
    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    private sealed class FakeFetch : IFetchClient
    {
        public List<FetchRequest> Calls { get; } = new();
        public Task<FetchPayload> FetchAsync(FetchRequest request, CancellationToken ct = default)
        {
            Calls.Add(request);
            return Task.FromResult(FetchPayload.Empty);
        }
    }

    private sealed class FakeWriter : IAdjustFactorWriter
    {
        public int Calls { get; private set; }
        public Task<int> PersistAsync(string code, DateOnly end, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
        {
            Calls++;
            return Task.FromResult(0);
        }
    }

    private sealed class FakeWatermarks(DataWatermark? wm) : IWatermarkStore
    {
        public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
            => Task.FromResult(wm);
    }

    [Fact]
    public async Task 首次触达_整段抓取_从1990到end()
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        var svc = new AdjustFactorService(new FakeWatermarks(null), fetch, writer);

        await svc.EnsureFullAsync("sh.600000", D(2026, 6, 10), NOW);

        Assert.Single(fetch.Calls);
        Assert.Equal("fetch_adjust_factor", fetch.Calls[0].Type);
        Assert.Equal(D(1990, 12, 19), fetch.Calls[0].StartDate);   // 恒从开市日
        Assert.Equal(D(2026, 6, 10), fetch.Calls[0].EndDate);
        Assert.Equal("sh.600000", fetch.Calls[0].Code);
        Assert.Equal(1, writer.Calls);
    }

    [Fact]
    public async Task 已覆盖且新鲜_不抓()
    {
        // adjust_factor 刷新间隔 300s：须新鲜(<5min)才不抓——因子盘中随新除权会变
        var wm = new DataWatermark
        {
            Code = "sh.600000", DataType = "adjust_factor",
            FirstDate = D(1990, 12, 19), LastDate = D(2026, 6, 11), LastFetchedAt = NOW.AddSeconds(-60),
        };
        var fetch = new FakeFetch();
        var svc = new AdjustFactorService(new FakeWatermarks(wm), fetch, new FakeWriter());

        await svc.EnsureFullAsync("sh.600000", D(2026, 6, 11), NOW);

        Assert.Empty(fetch.Calls);
    }
}
