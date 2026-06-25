namespace StockData.Mcp.Data;

/// <summary>
/// 超大缺口切片——移植自 <c>api/services/readthrough.py</c> 的 <c>_split_range</c> / <c>_SLICE_DAYS</c>。
/// 单任务时长有界，每段落库即推进水位（断点续传）。切片在 dotnet（TASK D-A：Python 只抓给定区间）。
/// </summary>
public static class RangeSlicer
{
    private static readonly IReadOnlyDictionary<string, int> SliceDaysMap = new Dictionary<string, int>
    {
        ["k_d"] = 3650, ["k_w"] = 3650, ["k_m"] = 3650,
        ["k_5"] = 730, ["k_15"] = 730, ["k_30"] = 730, ["k_60"] = 730,
    };

    /// <summary>该数据类型的切片上限（天/段）；未列出（日历/宏观等行数小）返回 null = 不切。</summary>
    public static int? SliceDays(string dataType)
        => SliceDaysMap.TryGetValue(dataType, out var v) ? v : null;

    /// <summary>把 [fs, fe] 切成跨度 ≤ maxDays 的连续闭区间，升序无缝衔接。maxDays=null 不切。</summary>
    public static IEnumerable<(DateOnly Start, DateOnly End)> Slice(DateOnly fs, DateOnly fe, int? maxDays)
    {
        if (maxDays is null)
        {
            yield return (fs, fe);
            yield break;
        }
        var step = maxDays.Value - 1;
        while (fs <= fe)
        {
            var cut = fs.AddDays(step);
            if (cut > fe) cut = fe;
            yield return (fs, cut);
            fs = cut.AddDays(1);
        }
    }
}
