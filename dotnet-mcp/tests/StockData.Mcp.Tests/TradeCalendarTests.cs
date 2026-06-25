using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

public class TradeCalendarParserTests
{
    [Fact]
    public void 解析日历_is_trading_day只有1为真()
    {
        var payload = new FetchPayload(
            new[] { "calendar_date", "is_trading_day" },
            new IReadOnlyList<string?>[]
            {
                new string?[] { "2024-01-01", "0" },   // 元旦休市
                new string?[] { "2024-01-02", "1" },
                new string?[] { "2024-01-06", "0" },   // 周六
            });

        var rows = TradeCalendarParser.Parse(payload);
        Assert.Equal(3, rows.Count);
        Assert.Equal((new DateOnly(2024, 1, 1), false), rows[0]);
        Assert.Equal((new DateOnly(2024, 1, 2), true), rows[1]);
        Assert.False(rows[2].IsTradingDay);
    }

    [Fact]
    public void 序列化_calendar_date与bool()
    {
        var json = TradeCalendarReadService.Serialize(new List<TradeCalendar>
        {
            new() { CalendarDate = new DateOnly(2024, 1, 1), IsTradingDay = false },
            new() { CalendarDate = new DateOnly(2024, 1, 2), IsTradingDay = true },
        });
        Assert.Contains("\"calendar_date\":\"2024-01-01\"", json);
        Assert.Contains("\"is_trading_day\":false", json);
        Assert.Contains("\"is_trading_day\":true", json);
    }
}

/// <summary>交易日历编排（TradeCalendarService）——fake 三件套验证 coverage→抓取→落盘。</summary>
public class TradeCalendarServiceTests
{
    private static readonly DateTimeOffset NOW = new(2026, 6, 11, 12, 0, 0, TimeSpan.FromHours(8));
    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    private sealed class FakeWatermarks(DataWatermark? wm) : IWatermarkStore
    {
        public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
            => Task.FromResult(wm);
    }

    private sealed class FakeFetch : IFetchClient
    {
        public List<FetchRequest> Calls { get; } = new();
        public Task<FetchPayload> FetchAsync(FetchRequest request, CancellationToken ct = default)
        {
            Calls.Add(request);
            return Task.FromResult(new FetchPayload(
                new[] { "calendar_date", "is_trading_day" },
                new IReadOnlyList<string?>[] { new string?[] { request.EndDate!.Value.ToString("yyyy-MM-dd"), "1" } }));
        }
    }

    private sealed class FakeWriter : ITradeCalendarWriter
    {
        public List<(DateOnly Start, DateOnly End)> Calls { get; } = new();
        public Task<int> PersistAsync(FetchPayload payload, DateOnly sliceStart, DateOnly sliceEnd, CancellationToken ct = default)
        {
            Calls.Add((sliceStart, sliceEnd));
            return Task.FromResult(payload.Rows.Count);
        }
    }

    [Fact]
    public async Task 首次触达_从1990整段抓取_不切片()
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        var svc = new TradeCalendarService(new FakeWatermarks(null), fetch, writer);

        await svc.EnsureRangeAsync(D(2024, 1, 1), D(2024, 1, 10), NOW);

        Assert.Single(fetch.Calls);                                  // 日历不切片
        Assert.Equal("fetch_trade_calendar", fetch.Calls[0].Type);
        Assert.Null(fetch.Calls[0].Code);                            // 全市场，无 code
        Assert.Equal(D(1990, 12, 19), fetch.Calls[0].StartDate);     // 回填起点
        Assert.Equal(D(2024, 1, 10), fetch.Calls[0].EndDate);
        Assert.Single(writer.Calls);
    }

    [Fact]
    public async Task 命中新鲜水位_不抓()
    {
        var wm = new DataWatermark
        {
            Code = "", DataType = "trade_calendar",
            FirstDate = D(1990, 12, 19), LastDate = D(2026, 12, 31), LastFetchedAt = NOW.AddSeconds(-60),
        };
        var fetch = new FakeFetch();
        var svc = new TradeCalendarService(new FakeWatermarks(wm), fetch, new FakeWriter());

        await svc.EnsureRangeAsync(D(2026, 1, 1), D(2026, 6, 30), NOW);

        Assert.Empty(fetch.Calls);
    }

    [Fact]
    public async Task 未来区间不钳制_抓到未来交易日()
    {
        // 日历可请求未来：水位到 2026-06-30，请求到年底 → 尾部缺口 (7/1, 12/31) 含未来
        var wm = new DataWatermark
        {
            Code = "", DataType = "trade_calendar",
            FirstDate = D(1990, 12, 19), LastDate = D(2026, 6, 30), LastFetchedAt = NOW.AddSeconds(-60),
        };
        var fetch = new FakeFetch();
        var svc = new TradeCalendarService(new FakeWatermarks(wm), fetch, new FakeWriter());

        await svc.EnsureRangeAsync(D(2026, 1, 1), D(2026, 12, 31), NOW);

        Assert.Single(fetch.Calls);
        Assert.Equal(D(2026, 7, 1), fetch.Calls[0].StartDate);
        Assert.Equal(D(2026, 12, 31), fetch.Calls[0].EndDate);       // 未来日未被钳制
    }
}
