using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>
/// 日线读穿透编排（KlineService.EnsureRange）端到端逻辑——用 fake 三件套验证
/// coverage → 切片 → 抓取 → 落盘 的串联，不触 Python/baostock/PG。
/// 固定"现在"2026-06-11 12:00 +08（与 CoverageTests 一致）。
/// </summary>
public class KlineServiceTests
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
            // 回一行业务日期 = 切片末，使 MaxDate 有值（编排不关心内容，writer 是 fake）
            var payload = new FetchPayload(
                new[] { "date" },
                new IReadOnlyList<string?>[] { new string?[] { request.EndDate!.Value.ToString("yyyy-MM-dd") } });
            return Task.FromResult(payload);
        }
    }

    private sealed record PersistCall(string Code, string Frequency, string DataType, DateOnly Start, DateOnly End);

    private sealed class FakeWriter : IKlineWriter
    {
        public List<PersistCall> Calls { get; } = new();
        public Task<int> PersistAsync(string code, string frequency, string dataType, FetchPayload payload,
            DateOnly sliceStart, DateOnly sliceEnd, DateTimeOffset now, CancellationToken ct = default)
        {
            Calls.Add(new(code, frequency, dataType, sliceStart, sliceEnd));
            return Task.FromResult(payload.Rows.Count);
        }
    }

    private static (KlineService svc, FakeFetch fetch, FakeWriter writer) Build(DataWatermark? wm)
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        return (new KlineService(new FakeWatermarks(wm), fetch, writer), fetch, writer);
    }

    [Fact]
    public async Task 命中新鲜水位_不抓不写()
    {
        var wm = new DataWatermark
        {
            Code = "sh.600000", DataType = "k_d",
            FirstDate = D(2020, 1, 1), LastDate = D(2024, 12, 31), LastFetchedAt = NOW.AddSeconds(-10_000_000),
        };
        var (svc, fetch, writer) = Build(wm);

        await svc.EnsureRangeAsync("sh.600000", "k_d", D(2024, 1, 1), D(2024, 12, 31), NOW);

        Assert.Empty(fetch.Calls);
        Assert.Empty(writer.Calls);
    }

    [Fact]
    public async Task 首次触达_从1990全史切片回填_逐段落库()
    {
        var (svc, fetch, writer) = Build(null);

        await svc.EnsureRangeAsync("sh.600000", "k_d", D(2024, 1, 1), D(2026, 6, 11), NOW);

        // 切片：1990-12-19 → 2026-6-11，3650 天/段 → 多段
        Assert.True(fetch.Calls.Count >= 4);
        Assert.Equal(fetch.Calls.Count, writer.Calls.Count);

        // 首段起点 = 回填起点，末段终点 = 请求尾
        Assert.Equal(D(1990, 12, 19), fetch.Calls[0].StartDate);
        Assert.Equal(D(2026, 6, 11), fetch.Calls[^1].EndDate);

        // 连续无缝 + frequency=d + 每段 fetch 与 persist 区间一致
        for (var i = 0; i < fetch.Calls.Count; i++)
        {
            Assert.Equal("d", fetch.Calls[i].Frequency);
            Assert.Equal(fetch.Calls[i].StartDate, writer.Calls[i].Start);
            Assert.Equal(fetch.Calls[i].EndDate, writer.Calls[i].End);
            Assert.Equal("k_d", writer.Calls[i].DataType);
            if (i > 0) Assert.Equal(fetch.Calls[i - 1].EndDate!.Value.AddDays(1), fetch.Calls[i].StartDate);
        }
    }

    [Fact]
    public async Task 尾部缺口_单段抓取该缺口()
    {
        var wm = new DataWatermark
        {
            Code = "sh.600000", DataType = "k_d",
            FirstDate = D(2020, 1, 1), LastDate = D(2025, 12, 31), LastFetchedAt = NOW.AddSeconds(-60),
        };
        var (svc, fetch, writer) = Build(wm);

        await svc.EnsureRangeAsync("sh.600000", "k_d", D(2025, 1, 1), D(2026, 6, 10), NOW);

        Assert.Single(fetch.Calls);
        Assert.Equal(D(2026, 1, 1), fetch.Calls[0].StartDate);
        Assert.Equal(D(2026, 6, 10), fetch.Calls[0].EndDate);
        Assert.Single(writer.Calls);
    }
}
