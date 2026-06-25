using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

public class FinancialParserTests
{
    [Fact]
    public void 季度类_拆record_提日期_其余进metrics()
    {
        // fetch_financial_report payload：fields=[report_type, record]，record 是该类 JSON 记录
        var payload = new FetchPayload(
            new[] { "report_type", "record" },
            new IReadOnlyList<string?>[]
            {
                new string?[] { "profit", """{"code":"sh.600000","statDate":"2024-03-31","pubDate":"2024-04-30","roeAvg":"0.05","npMargin":"0.12"}""" },
                new string?[] { "growth", """{"code":"sh.600000","statDate":"2024-03-31","pubDate":"2024-04-30","YOYEquity":"0.08"}""" },
            });

        var rows = FinancialParser.ParseQuarterly(payload);
        Assert.Equal(2, rows.Count);
        Assert.Equal("profit", rows[0].ReportType);
        Assert.Equal(new DateOnly(2024, 3, 31), rows[0].StatDate);
        Assert.Equal(new DateOnly(2024, 4, 30), rows[0].PubDate);
        // metrics 排除 code/statDate/pubDate
        Assert.Equal("""{"roeAvg":"0.05","npMargin":"0.12"}""", rows[0].MetricsJson);
        Assert.DoesNotContain("statDate", rows[0].MetricsJson);
    }

    [Fact]
    public void 季度类_缺report或record_跳过()
    {
        var payload = new FetchPayload(
            new[] { "report_type", "record" },
            new IReadOnlyList<string?>[] { new string?[] { "profit", """{"code":"c","pubDate":"2024-04-30"}""" } });  // 无 statDate
        Assert.Empty(FinancialParser.ParseQuarterly(payload));
    }

    [Fact]
    public void 快报_用专属stat_pub_key_其余进metrics()
    {
        var (statKey, pubKey) = FinancialWriter.PerfKeys("express");
        Assert.Equal("performanceExpStatDate", statKey);
        var payload = new FetchPayload(
            new[] { "code", "performanceExpStatDate", "performanceExpPubDate", "performanceExpressROEWa" },
            new IReadOnlyList<string?>[] { new string?[] { "sh.600000", "2024-03-31", "2024-04-15", "0.06" } });

        var rows = FinancialParser.ParsePerformance(payload, "express", statKey, pubKey);
        Assert.Single(rows);
        Assert.Equal(new DateOnly(2024, 3, 31), rows[0].StatDate);
        Assert.Equal("""{"performanceExpressROEWa":"0.06"}""", rows[0].MetricsJson);
    }
}

public class FinancialServiceTests
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

    private sealed class FakeWriter : IFinancialWriter
    {
        public int Perf { get; private set; }
        public Task PersistQuarterlyAsync(string code, int year, int quarter, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
            => Task.CompletedTask;
        public Task PersistPerformanceAsync(string code, string reportType, DateOnly start, DateOnly end, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
        { Perf++; return Task.CompletedTask; }
    }

    private sealed class FakeWatermarks(DataWatermark? wm) : IWatermarkStore
    {
        public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
            => Task.FromResult(wm);
    }

    [Fact]
    public async Task 快报_首次触达_范围抓取()
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        var svc = new PerformanceService(fetch, new FakeWatermarks(null), writer);

        await svc.EnsureAsync("sh.600000", "express", D(2024, 1, 1), D(2024, 12, 31), NOW);

        var req = Assert.Single(fetch.Calls);
        Assert.Equal("fetch_performance", req.Type);
        Assert.Equal("express", req.ReportType);
        Assert.Equal(1, writer.Perf);
    }
}
