using TALib;

namespace StockData.Mcp.Indicators;

/// <summary>
/// 指标注册表与计算引擎（TALib.NETCore 封装）。
///
/// 指标规格字符串：MA20 / EMA12 / RSI14 / ATR14 / CCI14（名称+周期，周期可省略用默认），
/// MACD / KDJ / BOLL / OBV（固定参数组）。
/// 输出按输入序列对齐（lookback 预热区为 null），KDJ 的 J = 3K - 2D（TA-Lib STOCH 不直接提供）。
/// </summary>
public static class IndicatorEngine
{
    public sealed record Spec(string Key, string Kind, int Period);

    private static readonly Dictionary<string, int> DefaultPeriods = new(StringComparer.OrdinalIgnoreCase)
    {
        ["MA"] = 20, ["EMA"] = 20, ["RSI"] = 14, ["ATR"] = 14, ["CCI"] = 14,
    };

    private static readonly string[] FixedKinds = ["MACD", "KDJ", "BOLL", "OBV"];

    /// <summary>解析逗号分隔的指标列表，如 "MA5,MA20,MACD,RSI14"。</summary>
    public static (List<Spec> Specs, string? Error) Parse(string indicators)
    {
        var specs = new List<Spec>();
        foreach (var raw in indicators.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var item = raw.ToUpperInvariant();
            if (FixedKinds.Contains(item))
            {
                specs.Add(new Spec(item, item, 0));
                continue;
            }
            var split = item.TakeWhile(char.IsLetter).Count();
            var kind = item[..split];
            if (!DefaultPeriods.TryGetValue(kind, out var defaultPeriod))
                return ([], $"Error: 不支持的指标 '{raw}'。可选: MA{{n}}/EMA{{n}}/RSI{{n}}/ATR{{n}}/CCI{{n}}/MACD/KDJ/BOLL/OBV");
            var period = defaultPeriod;
            if (split < item.Length && !int.TryParse(item[split..], out period))
                return ([], $"Error: 指标周期无法解析 '{raw}'");
            if (period is < 2 or > 250)
                return ([], $"Error: 指标周期超出范围(2-250) '{raw}'");
            specs.Add(new Spec(item, kind, period));
        }
        return specs.Count > 0 ? (specs, null) : ([], "Error: 未指定任何指标");
    }

    /// <summary>该指标的预热长度（TA-Lib lookback + EMA 族额外余量保证收敛）。</summary>
    public static int Lookback(Spec s) => s.Kind switch
    {
        "MA" => Functions.SmaLookback(s.Period),
        "EMA" => Functions.EmaLookback(s.Period) + s.Period * 3, // EMA 受初值影响，多取保证收敛
        "RSI" => Functions.RsiLookback(s.Period) + s.Period * 3,
        "ATR" => Functions.AtrLookback(s.Period) + s.Period * 3,
        "CCI" => Functions.CciLookback(s.Period),
        "MACD" => Functions.MacdLookback(12, 26, 9) + 100, // 慢线 EMA26 收敛余量
        "KDJ" => Functions.StochLookback(9, 3, Core.MAType.Sma, 3, Core.MAType.Sma),
        "BOLL" => Functions.BbandsLookback(20, Core.MAType.Sma),
        "OBV" => 0,
        _ => 0,
    };

    public static int MaxLookback(IEnumerable<Spec> specs) => specs.Max(Lookback);

    /// <summary>计算单个指标，返回 输出名 → 与输入等长的序列（预热区 null）。</summary>
    public static Dictionary<string, double?[]> Compute(Spec s, KlineSeries k)
    {
        var n = k.Length;
        var range = new Range(0, n - 1); // TALib 沿用 C 版闭区间 endIdx，非 .NET 切片语义
        switch (s.Kind)
        {
            case "MA":
            {
                var output = new double[n];
                Functions.Sma<double>(k.Close, range, output, out var outRange, s.Period);
                return new() { [s.Key] = Align(output, outRange, n) };
            }
            case "EMA":
            {
                var output = new double[n];
                Functions.Ema<double>(k.Close, range, output, out var outRange, s.Period);
                return new() { [s.Key] = Align(output, outRange, n) };
            }
            case "RSI":
            {
                var output = new double[n];
                Functions.Rsi<double>(k.Close, range, output, out var outRange, s.Period);
                return new() { [s.Key] = Align(output, outRange, n) };
            }
            case "ATR":
            {
                var output = new double[n];
                Functions.Atr<double>(k.High, k.Low, k.Close, range, output, out var outRange, s.Period);
                return new() { [s.Key] = Align(output, outRange, n) };
            }
            case "CCI":
            {
                var output = new double[n];
                Functions.Cci<double>(k.High, k.Low, k.Close, range, output, out var outRange, s.Period);
                return new() { [s.Key] = Align(output, outRange, n) };
            }
            case "OBV":
            {
                var output = new double[n];
                Functions.Obv<double>(k.Close, k.Volume, range, output, out var outRange);
                return new() { [s.Key] = Align(output, outRange, n) };
            }
            case "MACD":
            {
                var dif = new double[n];
                var dea = new double[n];
                var hist = new double[n];
                Functions.Macd<double>(k.Close, range, dif, dea, hist, out var outRange, 12, 26, 9);
                return new()
                {
                    ["MACD_DIF"] = Align(dif, outRange, n),
                    ["MACD_DEA"] = Align(dea, outRange, n),
                    ["MACD_HIST"] = Align(hist, outRange, n),
                };
            }
            case "KDJ":
            {
                var kOut = new double[n];
                var dOut = new double[n];
                Functions.Stoch<double>(k.High, k.Low, k.Close, range, kOut, dOut, out var outRange,
                    9, 3, Core.MAType.Sma, 3, Core.MAType.Sma);
                var kAligned = Align(kOut, outRange, n);
                var dAligned = Align(dOut, outRange, n);
                var j = new double?[n];
                for (var i = 0; i < n; i++)
                    if (kAligned[i] is { } kv && dAligned[i] is { } dv)
                        j[i] = 3 * kv - 2 * dv; // 国内口径 J 值，TA-Lib 不直接提供
                return new() { ["KDJ_K"] = kAligned, ["KDJ_D"] = dAligned, ["KDJ_J"] = j };
            }
            case "BOLL":
            {
                var upper = new double[n];
                var middle = new double[n];
                var lower = new double[n];
                Functions.Bbands<double>(k.Close, range, upper, middle, lower, out var outRange,
                    20, 2.0, 2.0, Core.MAType.Sma);
                return new()
                {
                    ["BOLL_UPPER"] = Align(upper, outRange, n),
                    ["BOLL_MIDDLE"] = Align(middle, outRange, n),
                    ["BOLL_LOWER"] = Align(lower, outRange, n),
                };
            }
            default:
                throw new InvalidOperationException($"未注册的指标类型 {s.Kind}");
        }
    }

    /// <summary>TA-Lib 输出从输入下标 outRange.Start 起：回铺为与输入等长、预热区 null 的序列。</summary>
    internal static double?[] Align(double[] output, Range outRange, int inputLength)
    {
        var aligned = new double?[inputLength];
        var begin = outRange.Start.Value;
        var count = outRange.End.Value - begin;
        for (var i = 0; i < count; i++)
            aligned[begin + i] = output[i];
        return aligned;
    }
}
