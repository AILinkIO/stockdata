using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>快照编排（SnapshotService / CheckSnapshot 样板）——fake ingest 验证各判定分支是否触发抓取。</summary>
public class SnapshotServiceTests
{
    private static readonly DateTimeOffset NOW = new(2026, 6, 11, 12, 0, 0, TimeSpan.FromHours(8));
    private static readonly DateOnly TODAY = new(2026, 6, 11);
    private static DateOnly D(int y, int m, int d) => new(y, m, d);

    private sealed class FakeIngest(bool hasRows) : ISnapshotIngest
    {
        public string DataType => "stock_list";
        public string WatermarkCode => "";
        public int PersistCalls { get; private set; }
        public Task<bool> HasRowsAsync(DateOnly snapDate, CancellationToken ct) => Task.FromResult(hasRows);
        public FetchRequest BuildRequest(DateOnly snapDate) => new("fetch_stock_list", SnapDate: snapDate);
        public Task<int> PersistAsync(DateOnly snapDate, FetchPayload payload, CancellationToken ct)
        {
            PersistCalls++;
            return Task.FromResult(0);
        }
    }

    private sealed class FakeFetch : IFetchClient
    {
        public List<FetchRequest> Calls { get; } = new();
        public Task<FetchPayload> FetchAsync(FetchRequest request, CancellationToken ct = default)
        {
            Calls.Add(request);
            return Task.FromResult(FetchPayload.Empty);
        }
    }

    private sealed class FakeWatermarks(DataWatermark? wm) : IWatermarkStore
    {
        public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
            => Task.FromResult(wm);
    }

    private static async Task<(int fetches, int persists)> Run(bool hasRows, DateOnly snapDate, DataWatermark? wm)
    {
        var fetch = new FakeFetch();
        var ingest = new FakeIngest(hasRows);
        var svc = new SnapshotService(fetch, new FakeWatermarks(wm));
        await svc.EnsureSnapshotAsync(ingest, snapDate, NOW);
        return (fetch.Calls.Count, ingest.PersistCalls);
    }

    private static DataWatermark Wm(long fetchedAgoSeconds) => new()
    {
        Code = "", DataType = "stock_list", LastDate = TODAY, LastFetchedAt = NOW.AddSeconds(-fetchedAgoSeconds),
    };

    [Fact]
    public async Task 快照不存在_抓取() => Assert.Equal((1, 1), await Run(hasRows: false, TODAY, Wm(60)));

    [Fact]
    public async Task 历史快照_永久有效_不抓()
        => Assert.Equal((0, 0), await Run(hasRows: true, D(2026, 6, 5), Wm(10_000_000)));

    [Fact]
    public async Task 今日快照_无水位_抓() => Assert.Equal((1, 1), await Run(hasRows: true, TODAY, null));

    [Fact]
    public async Task 今日快照_过期_抓()                     // stock_list 刷新间隔 1 天
        => Assert.Equal((1, 1), await Run(hasRows: true, TODAY, Wm(2 * 86400)));

    [Fact]
    public async Task 今日快照_新鲜_不抓() => Assert.Equal((0, 0), await Run(hasRows: true, TODAY, Wm(3600)));
}

public class StockBasicSerializeTests
{
    [Fact]
    public void 序列化_单对象_null列保留()
    {
        var json = StockBasicReadService.Serialize(new StockBasic
        {
            Code = "sh.600000", CodeName = "浦发银行",
            IpoDate = new DateOnly(1999, 11, 10), OutDate = null, Type = 1, Status = 1,
        });
        Assert.Contains("\"code\":\"sh.600000\"", json);
        Assert.Contains("\"code_name\":\"浦发银行\"", json);
        Assert.Contains("\"ipo_date\":\"1999-11-10\"", json);
        Assert.Contains("\"out_date\":null", json);
        Assert.Contains("\"type\":1", json);
    }
}
