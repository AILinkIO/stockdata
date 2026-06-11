using TALib;

namespace StockData.Mcp.Indicators;

/// <summary>
/// K 线形态识别（TALib.Candles 封装）。
/// 输出值约定：+100 看涨形态命中、-100 看跌形态命中、0 未命中
/// （部分形态如 Doji 只输出 ±100 中的一种）。
/// </summary>
public static class PatternEngine
{
    public delegate Core.RetCode CandleFn(
        ReadOnlySpan<double> open, ReadOnlySpan<double> high,
        ReadOnlySpan<double> low, ReadOnlySpan<double> close,
        Range inRange, Span<int> output, out Range outRange);

    /// <summary>常用形态注册表：名称 → (中文名, 函数)。</summary>
    public static readonly Dictionary<string, (string Cn, CandleFn Fn)> Patterns =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["Doji"] = ("十字星", Candles.Doji),
            ["DragonflyDoji"] = ("蜻蜓十字", Candles.DragonflyDoji),
            ["GravestoneDoji"] = ("墓碑十字", Candles.GravestoneDoji),
            ["Hammer"] = ("锤子线", Candles.Hammer),
            ["HangingMan"] = ("上吊线", Candles.HangingMan),
            ["InvertedHammer"] = ("倒锤子", Candles.InvertedHammer),
            ["ShootingStar"] = ("流星线", Candles.ShootingStar),
            ["Engulfing"] = ("吞没形态", Candles.Engulfing),
            ["Harami"] = ("孕线", Candles.Harami),
            ["MorningStar"] = ("早晨之星", (ReadOnlySpan<double> o, ReadOnlySpan<double> h,
                ReadOnlySpan<double> l, ReadOnlySpan<double> c, Range r, Span<int> output,
                out Range outR) => Candles.MorningStar(o, h, l, c, r, output, out outR)),
            ["EveningStar"] = ("黄昏之星", (ReadOnlySpan<double> o, ReadOnlySpan<double> h,
                ReadOnlySpan<double> l, ReadOnlySpan<double> c, Range r, Span<int> output,
                out Range outR) => Candles.EveningStar(o, h, l, c, r, output, out outR)),
            ["ThreeWhiteSoldiers"] = ("红三兵", Candles.ThreeWhiteSoldiers),
            ["ThreeBlackCrows"] = ("三只乌鸦", Candles.ThreeBlackCrows),
            ["DarkCloudCover"] = ("乌云盖顶", (ReadOnlySpan<double> o, ReadOnlySpan<double> h,
                ReadOnlySpan<double> l, ReadOnlySpan<double> c, Range r, Span<int> output,
                out Range outR) => Candles.DarkCloudCover(o, h, l, c, r, output, out outR)),
            ["Piercing"] = ("刺透形态", Candles.PiercingLine),
        };

    public sealed record Hit(string Date, string Pattern, string PatternCn, string Signal);

    /// <summary>对序列识别指定形态（null = 全部注册形态），返回命中列表。</summary>
    public static List<Hit> Detect(KlineSeries k, IReadOnlyCollection<string>? patternNames,
        int fromIndex)
    {
        var names = patternNames is { Count: > 0 }
            ? patternNames.ToArray()
            : Patterns.Keys.ToArray();
        var hits = new List<Hit>();
        var range = new Range(0, k.Length - 1); // TALib 闭区间 endIdx
        foreach (var name in names)
        {
            if (!Patterns.TryGetValue(name, out var entry))
                continue; // 工具层已校验，此处静默跳过
            var output = new int[k.Length];
            var ret = entry.Fn(k.Open, k.High, k.Low, k.Close, range, output, out var outRange);
            if (ret != Core.RetCode.Success) continue;
            var begin = outRange.Start.Value;
            var count = outRange.End.Value - begin;
            for (var i = 0; i < count; i++)
            {
                if (output[i] == 0) continue;
                var idx = begin + i;
                if (idx < fromIndex) continue; // lookback 预热区不输出
                hits.Add(new Hit(k.Dates[idx], name, entry.Cn,
                    output[i] > 0 ? "bullish" : "bearish"));
            }
        }
        return hits.OrderBy(h => h.Date).ThenBy(h => h.Pattern).ToList();
    }

    /// <summary>形态识别的最大 lookback（三日形态 + 平均周期），统一取 30 根余量。</summary>
    public const int MaxLookback = 30;
}
