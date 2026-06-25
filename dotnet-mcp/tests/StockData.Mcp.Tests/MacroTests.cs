using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

public class MacroParserTests
{
    [Fact]
    public void 利率类_字段转换()
    {
        var spec = MacroSpecs.All["deposit_rate"];
        var payload = new FetchPayload(
            new[] { "pubDate", "demandDepositRate", "fixedDepositRate3Month" },
            new IReadOnlyList<string?>[] { new string?[] { "2024-01-01", "0.35", "1.25" } });

        var rows = MacroParser.Parse(spec, payload);
        Assert.Single(rows);
        Assert.Equal(new DateOnly(2024, 1, 1), rows[0][0]);   // pub_date
        Assert.Equal(0.35m, rows[0][1]);                       // demand_deposit_rate
        Assert.Equal(1.25m, rows[0][2]);                       // fixed_deposit_rate_3month
    }

    [Fact]
    public void 货币供应月_statYear_statMonth为short()
    {
        var spec = MacroSpecs.All["money_supply_month"];
        var payload = new FetchPayload(
            new[] { "statYear", "statMonth", "m0Month", "m0YOY" },
            new IReadOnlyList<string?>[] { new string?[] { "2024", "3", "10.5", "2.1" } });

        var rows = MacroParser.Parse(spec, payload);
        Assert.Equal((short)2024, rows[0][0]);
        Assert.Equal((short)3, rows[0][1]);
        Assert.Equal(10.5m, rows[0][2]);
    }

    [Fact]
    public void 主键缺失_丢行()
    {
        var spec = MacroSpecs.All["deposit_rate"];
        var payload = new FetchPayload(
            new[] { "pubDate", "demandDepositRate" },
            new IReadOnlyList<string?>[] { new string?[] { "", "0.35" } });   // pub_date 空
        Assert.Empty(MacroParser.Parse(spec, payload));
    }
}

public class MacroServiceTests
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

    private sealed class FakeWriter : IMacroWriter
    {
        public List<(string Kind, DateOnly First, DateOnly Last)> Calls { get; } = new();
        public Task<int> PersistAsync(string kind, DateOnly first, DateOnly last, FetchPayload payload, CancellationToken ct = default)
        {
            Calls.Add((kind, first, last));
            return Task.FromResult(0);
        }
    }

    private sealed class FakeWatermarks : IWatermarkStore
    {
        public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
            => Task.FromResult<DataWatermark?>(null);
    }

    [Fact]
    public async Task 利率类_用ISO日期抓取()
    {
        var fetch = new FakeFetch();
        var svc = new MacroService(new FakeWatermarks(), fetch, new FakeWriter());
        await svc.EnsureRangeAsync("deposit_rate", D(2024, 1, 1), D(2024, 6, 1), NOW);

        var req = Assert.Single(fetch.Calls);
        Assert.Equal("fetch_macro", req.Type);
        Assert.Equal("deposit_rate", req.Kind);
        var ps = req.ToParams();
        Assert.Equal("1990-12-19", ps["start_date"]);   // 首次触达全史回填，ISO 日期格式（对比货币供应的 YYYY-MM）
    }

    [Fact]
    public async Task 货币供应月_用YYYY_MM抓取_水位折为月初()
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        var svc = new MacroService(new FakeWatermarks(), fetch, writer);
        // 首次触达 money_supply_month → 全史回填 [1990-12-19, 2024-06-01]，单段（不切片）
        await svc.EnsureRangeAsync("money_supply_month", D(2024, 1, 1), D(2024, 6, 1), NOW);

        var ps = fetch.Calls[0].ToParams();
        Assert.Equal("1990-12", ps["start_date"]);   // YYYY-MM，非 ISO 日期
        Assert.Equal("2024-06", ps["end_date"]);
        // 水位折为月初
        Assert.Equal(D(1990, 12, 1), writer.Calls[0].First);
        Assert.Equal(D(2024, 6, 1), writer.Calls[0].Last);
    }

    [Fact]
    public async Task 货币供应年_用YYYY抓取()
    {
        var fetch = new FakeFetch();
        var svc = new MacroService(new FakeWatermarks(), fetch, new FakeWriter());
        await svc.EnsureRangeAsync("money_supply_year", D(2020, 1, 1), D(2024, 12, 31), NOW);

        var ps = fetch.Calls[0].ToParams();
        Assert.Equal("1990", ps["start_date"]);   // YYYY
        Assert.Equal("2024", ps["end_date"]);
    }
}
