using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

public class KlineMinuteParserTests
{
    [Fact]
    public void bar_time_取前14位_加8时区()
    {
        var bt = KlineMinuteParser.BarTime("20240102093500000");
        Assert.NotNull(bt);
        Assert.Equal(new DateTimeOffset(2024, 1, 2, 9, 35, 0, TimeSpan.FromHours(8)), bt!.Value);
    }

    [Fact]
    public void 短串或空_返回null()
    {
        Assert.Null(KlineMinuteParser.BarTime("2024"));
        Assert.Null(KlineMinuteParser.BarTime(null));
    }

    [Fact]
    public void 解析_映射字段_跳过无time行()
    {
        var payload = new FetchPayload(
            new[] { "time", "code", "open", "high", "low", "close", "volume", "amount" },
            new IReadOnlyList<string?>[]
            {
                new string?[] { "20240102093500000", "sh.600000", "10.2", "10.5", "10.1", "10.4", "1000", "10400" },
                new string?[] { "", "sh.600000", "1", "1", "1", "1", "1", "1" },   // 无 time → 跳过
            });
        var rows = KlineMinuteParser.Parse(payload, "sh.600000", 5);
        Assert.Single(rows);
        Assert.Equal(10.5m, rows[0].High);
        Assert.Equal(1000L, rows[0].Volume);
        Assert.Equal((short)5, rows[0].Frequency);
        Assert.Equal(new DateOnly(2024, 1, 2), KlineMinuteParser.MaxDate(rows));
    }
}

public class KlineMinuteServiceTests
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

    private sealed class FakeWriter : IKlineMinuteWriter
    {
        public List<(DateOnly Start, DateOnly End)> Calls { get; } = new();
        public Task<int> PersistAsync(string code, short frequency, string dataType, FetchPayload payload,
            DateOnly sliceStart, DateOnly sliceEnd, DateTimeOffset now, CancellationToken ct = default)
        {
            Calls.Add((sliceStart, sliceEnd));
            return Task.FromResult(0);
        }
    }

    private sealed class FakeWatermarks(DataWatermark? wm) : IWatermarkStore
    {
        public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
            => Task.FromResult(wm);
    }

    [Fact]
    public async Task 首次触达_从分钟回填起点_切片730_复用fetch_kline()
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        var svc = new KlineMinuteService(new FakeWatermarks(null), fetch, writer);

        await svc.EnsureRangeAsync("sh.600000", 5, D(2024, 1, 1), D(2026, 6, 10), NOW);

        // 分钟回填起点 2023-01-01，切片 730 天/段 → 多段
        Assert.True(fetch.Calls.Count >= 2);
        Assert.Equal("fetch_kline", fetch.Calls[0].Type);   // 复用 fetch_kline
        Assert.Equal("5", fetch.Calls[0].Frequency);
        Assert.Equal(D(2023, 1, 1), fetch.Calls[0].StartDate);
        Assert.Equal(fetch.Calls.Count, writer.Calls.Count);
        for (var i = 1; i < fetch.Calls.Count; i++)
            Assert.Equal(fetch.Calls[i - 1].EndDate!.Value.AddDays(1), fetch.Calls[i].StartDate);
    }

    [Fact]
    public async Task 命中新鲜_不抓()
    {
        var wm = new DataWatermark
        {
            Code = "sh.600000", DataType = "k_5",
            FirstDate = D(2023, 1, 1), LastDate = D(2026, 6, 11), LastFetchedAt = NOW.AddSeconds(-60),
        };
        var fetch = new FakeFetch();
        var svc = new KlineMinuteService(new FakeWatermarks(wm), fetch, new FakeWriter());

        await svc.EnsureRangeAsync("sh.600000", 5, D(2026, 6, 1), D(2026, 6, 11), NOW);
        Assert.Empty(fetch.Calls);
    }
}
