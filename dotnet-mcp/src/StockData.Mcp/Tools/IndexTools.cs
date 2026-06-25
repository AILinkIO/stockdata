using System.ComponentModel;
using System.Text.Encodings.Web;
using System.Text.Json;
using ModelContextProtocol.Server;
using StockData.Mcp.Data;
using StockData.Mcp.StockDataClient;

namespace StockData.Mcp.Tools;

/// <summary>指数成分股与行业分类（管线开→dotnet 快照；关→旧 REST）。</summary>
[McpServerToolType]
public static class IndexTools
{
    private static readonly Dictionary<string, string> IndexAlias = new(StringComparer.OrdinalIgnoreCase)
    {
        ["sz50"] = "sz50", ["sse50"] = "sz50", ["000016"] = "sz50",
        ["hs300"] = "hs300", ["csi300"] = "hs300", ["000300"] = "hs300",
        ["zz500"] = "zz500", ["csi500"] = "zz500", ["000905"] = "zz500",
    };

    private static DateOnly? Parse(string? date) => date is not null && DateOnly.TryParse(date, out var d) ? d : null;

    private static Task<string> Constituents(StockDataApiClient api, SnapshotReadService snap, string index,
        string? date, int limit, CancellationToken ct)
        => snap.Enabled
            ? Wrap(snap.IndexConstituentsJsonAsync(index, Parse(date), ct), limit)
            : Wrap(api.GetAsync($"/api/v1/indices/{index}/constituents", new() { ["snap_date"] = date }, ct), limit);

    private static Task<string> IndustryBase(StockDataApiClient api, SnapshotReadService snap, string? code, string? date, CancellationToken ct)
        => snap.Enabled
            ? snap.IndustryJsonAsync(code, Parse(date), ct)
            : api.GetAsync("/api/v1/industries", new() { ["code"] = code, ["snap_date"] = date }, ct);

    private static async Task<string> Wrap(Task<string> json, int limit) => JsonHelper.Truncate(await json, limit);

    [McpServerTool(Name = "get_index_constituents")]
    [Description("获取指数成分股。index: sz50/hs300/zz500（或 000016/000300/000905、SSE50/CSI300/CSI500 等别名）。")]
    public static Task<string> GetIndexConstituents(StockDataApiClient api, SnapshotReadService snap,
        [Description("指数：sz50/hs300/zz500 或别名")] string index,
        [Description("基准日 YYYY-MM-DD，缺省最新交易日")] string? date = null,
        int limit = 600, CancellationToken ct = default)
        => IndexAlias.TryGetValue(index.Trim(), out var key)
            ? Constituents(api, snap, key, date, limit, ct)
            : Task.FromResult($"Error: 不支持的指数 '{index}'，可选: sz50/hs300/zz500");

    [McpServerTool(Name = "get_sz50_stocks")]
    [Description("获取上证50成分股。")]
    public static Task<string> GetSz50(StockDataApiClient api, SnapshotReadService snap, string? date = null,
        int limit = 250, CancellationToken ct = default)
        => Constituents(api, snap, "sz50", date, limit, ct);

    [McpServerTool(Name = "get_hs300_stocks")]
    [Description("获取沪深300成分股。")]
    public static Task<string> GetHs300(StockDataApiClient api, SnapshotReadService snap, string? date = null,
        int limit = 350, CancellationToken ct = default)
        => Constituents(api, snap, "hs300", date, limit, ct);

    [McpServerTool(Name = "get_zz500_stocks")]
    [Description("获取中证500成分股。")]
    public static Task<string> GetZz500(StockDataApiClient api, SnapshotReadService snap, string? date = null,
        int limit = 550, CancellationToken ct = default)
        => Constituents(api, snap, "zz500", date, limit, ct);

    [McpServerTool(Name = "get_stock_industry")]
    [Description("获取行业分类信息。code 为空时返回全部股票的行业分类。")]
    public static async Task<string> GetStockIndustry(StockDataApiClient api, SnapshotReadService snap,
        string? code = null, string? date = null, int limit = 250, CancellationToken ct = default)
        => JsonHelper.Truncate(await IndustryBase(api, snap, code, date, ct), limit);

    [McpServerTool(Name = "list_industries")]
    [Description("列出全部行业名称及成分股数量。")]
    public static async Task<string> ListIndustries(StockDataApiClient api, SnapshotReadService snap,
        string? date = null, CancellationToken ct = default)
    {
        var json = await IndustryBase(api, snap, null, date, ct);
        if (json.StartsWith("Error:")) return json;
        try
        {
            using var doc = JsonDocument.Parse(json);
            var counts = new SortedDictionary<string, int>();
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                var industry = el.Str("industry") ?? "未分类";
                counts[industry] = counts.GetValueOrDefault(industry) + 1;
            }
            return JsonSerializer.Serialize(counts,
                new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        }
        catch (JsonException)
        {
            return json;
        }
    }

    [McpServerTool(Name = "get_industry_members")]
    [Description("获取某行业的全部成分股（行业名支持子串匹配，如\"货币金融\"）。")]
    public static async Task<string> GetIndustryMembers(StockDataApiClient api, SnapshotReadService snap,
        [Description("行业名（子串匹配）")] string industry,
        string? date = null, int limit = 250, CancellationToken ct = default)
    {
        var json = await IndustryBase(api, snap, null, date, ct);
        return JsonHelper.FilterArray(json,
            el => (el.Str("industry") ?? "").Contains(industry),
            limit, $"Error: 未找到行业 '{industry}'");
    }
}
