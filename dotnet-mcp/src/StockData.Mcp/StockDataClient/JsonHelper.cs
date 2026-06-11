using System.Text;
using System.Text.Json;

namespace StockData.Mcp.StockDataClient;

/// <summary>
/// 透传结果的轻量后处理。基于 JsonDocument 的 GetRawText 操作，
/// 数值保持原始文本不经 double 反序列化（精度约束见 TASK.md）。
/// </summary>
public static class JsonHelper
{
    /// <summary>数组结果按 limit 截断，附截断提示；非数组/错误字符串原样返回。</summary>
    public static string Truncate(string json, int limit)
    {
        if (json.StartsWith("Error:") || limit <= 0) return json;
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return json;
            var total = doc.RootElement.GetArrayLength();
            if (total <= limit) return json;

            var sb = new StringBuilder("[");
            var i = 0;
            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (i++ >= limit) break;
                if (i > 1) sb.Append(',');
                sb.Append(el.GetRawText());
            }
            sb.Append(']');
            return $"{sb}\n（共 {total} 行，已截断为前 {limit} 行）";
        }
        catch (JsonException)
        {
            return json;
        }
    }

    /// <summary>解析数组并按谓词过滤（保持元素原始文本），返回 JSON 数组文本。</summary>
    public static string FilterArray(string json, Func<JsonElement, bool> predicate,
        int limit, string emptyMessage)
    {
        if (json.StartsWith("Error:")) return json;
        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Array) return json;
            var hits = doc.RootElement.EnumerateArray().Where(predicate)
                .Select(e => e.GetRawText()).ToList();
            if (hits.Count == 0) return emptyMessage;
            var body = "[" + string.Join(",", hits.Take(limit)) + "]";
            return hits.Count > limit
                ? $"{body}\n（共 {hits.Count} 行，已截断为前 {limit} 行）"
                : body;
        }
        catch (JsonException)
        {
            return json;
        }
    }

    public static string? Str(this JsonElement e, string prop)
        => e.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString() : null;
}
