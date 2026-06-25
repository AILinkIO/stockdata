using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

public class DividendParserTests
{
    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    [Fact]
    public void 解析_典型列_detail收纳其余字段()
    {
        var payload = new FetchPayload(
            new[] { "code", "dividPlanAnnounceDate", "dividRegistDate", "dividOperateDate",
                    "dividCashPsBeforeTax", "dividCashPsAfterTax", "dividType" },
            new IReadOnlyList<string?>[]
            {
                new string?[] { "sh.600000", "2023-06-01", "2023-06-15", "2023-06-16", "0.5", "0.45", "现金分红" },
            });

        var rows = DividendParser.Parse(payload, "sh.600000", 2023, "report");
        Assert.Single(rows);
        var r = rows[0];
        // 列顺序：code, plan_announce_date, year_type, year, regist, operate, pay, cashBefore, cashAfter, stocks, reserve, detail
        Assert.Equal("sh.600000", r[0]);
        Assert.Equal(D(2023, 6, 1), r[1]);
        Assert.Equal("report", r[2]);
        Assert.Equal((short)2023, r[3]);
        Assert.Equal(D(2023, 6, 15), r[4]);
        Assert.Equal(0.5m, r[7]);
        Assert.Null(r[6]);                                  // pay_date 缺 → null
        Assert.Equal("{\"dividType\":\"现金分红\"}", r[11]); // detail 收纳未知字段(中文原样)
    }

    [Fact]
    public void 缺预案公告日_跳过()
    {
        var payload = new FetchPayload(
            new[] { "dividPlanAnnounceDate", "dividType" },
            new IReadOnlyList<string?>[] { new string?[] { "", "x" } });
        Assert.Empty(DividendParser.Parse(payload, "c", 2023, "report"));
    }

    [Fact]
    public void 无额外字段_detail为null()
    {
        var payload = new FetchPayload(
            new[] { "code", "dividPlanAnnounceDate", "dividStocksPs" },
            new IReadOnlyList<string?>[] { new string?[] { "c", "2023-06-01", "0.1" } });
        Assert.Null(DividendParser.Parse(payload, "c", 2023, "report")[0][11]);
    }
}

public class DividendServiceTests
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

    private sealed class FakeWriter : IDividendWriter
    {
        public int Calls { get; private set; }
        public Task<int> PersistAsync(string code, int year, string yearType, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
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
    public async Task 首次触达某年_抓取该年()
    {
        var fetch = new FakeFetch();
        var writer = new FakeWriter();
        var svc = new DividendService(new FakeWatermarks(null), fetch, writer);

        await svc.EnsureAsync("sh.600000", 2023, "report", NOW);

        Assert.Single(fetch.Calls);
        Assert.Equal("fetch_dividend", fetch.Calls[0].Type);
        Assert.Equal("sh.600000", fetch.Calls[0].Code);
        Assert.Equal("2023", fetch.Calls[0].Year);
        Assert.Equal("report", fetch.Calls[0].YearType);
        Assert.Equal(1, writer.Calls);
    }

    [Fact]
    public async Task 该年已覆盖且新鲜_不抓()
    {
        var wm = new DataWatermark
        {
            Code = "sh.600000", DataType = "dividend",
            FirstDate = D(2020, 1, 1), LastDate = D(2024, 12, 31), LastFetchedAt = NOW.AddSeconds(-60),
        };
        var fetch = new FakeFetch();
        var svc = new DividendService(new FakeWatermarks(wm), fetch, new FakeWriter());

        await svc.EnsureAsync("sh.600000", 2023, "report", NOW);   // 2023 在 [2020,2024] 内
        Assert.Empty(fetch.Calls);
    }
}

public class DividendSerializeTests
{
    [Fact]
    public void 序列化_detail作为嵌套json()
    {
        var json = DividendReadService.Serialize(new List<Dividend>
        {
            new()
            {
                Code = "sh.600000", Year = 2023, YearType = "report", PlanAnnounceDate = new DateOnly(2023, 6, 1),
                CashPsBeforeTax = 0.5m, PayDate = null, Detail = "{\"dividType\":\"现金分红\"}",
            },
        });
        Assert.Contains("\"cash_ps_before_tax\":0.5", json);
        Assert.Contains("\"pay_date\":null", json);
        Assert.Contains("\"detail\":{\"dividType\":\"现金分红\"}", json);  // 嵌套对象，非字符串
    }
}
