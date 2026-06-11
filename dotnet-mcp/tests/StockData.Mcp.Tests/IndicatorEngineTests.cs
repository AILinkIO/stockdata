using StockData.Mcp.Indicators;
using Xunit;

public class IndicatorEngineTests
{
    private static KlineSeries Synthetic(int n, Func<int, double>? close = null)
    {
        close ??= i => 10 + Math.Sin(i / 5.0) * 2 + i * 0.01;
        var dates = Enumerable.Range(0, n)
            .Select(i => new DateTime(2024, 1, 1).AddDays(i).ToString("yyyy-MM-dd")).ToArray();
        var c = Enumerable.Range(0, n).Select(close).ToArray();
        var high = c.Select(v => v * 1.02).ToArray();
        var low = c.Select(v => v * 0.98).ToArray();
        var open = c.Select(v => v * 0.999).ToArray();
        var vol = Enumerable.Range(0, n).Select(i => 1e6 + i * 1000.0).ToArray();
        return new KlineSeries(dates, open, high, low, c, vol);
    }

    // ── 规格解析 ──

    [Fact]
    public void Parse_ValidSpecs()
    {
        var (specs, err) = IndicatorEngine.Parse("MA5, ma20, MACD, RSI14, KDJ, BOLL, ATR, obv");
        Assert.Null(err);
        Assert.Equal(8, specs.Count);
        Assert.Equal(("MA5", "MA", 5), (specs[0].Key, specs[0].Kind, specs[0].Period));
        Assert.Equal(14, specs[3].Period);
        Assert.Equal(14, specs.First(s => s.Kind == "ATR").Period); // 缺省周期
    }

    [Theory]
    [InlineData("XYZ")]
    [InlineData("MA1")]      // 周期下界
    [InlineData("RSI999")]   // 周期上界
    [InlineData("")]
    public void Parse_InvalidSpecs(string input)
    {
        var (_, err) = IndicatorEngine.Parse(input);
        Assert.NotNull(err);
        Assert.StartsWith("Error:", err);
    }

    // ── 输出对齐 ──

    [Fact]
    public void Align_MapsOutRangeToInputIndices()
    {
        var aligned = IndicatorEngine.Align([1.0, 2.0, 3.0], new Range(4, 7), 8);
        Assert.Equal([null, null, null, null, 1.0, 2.0, 3.0, null], aligned);
    }

    // ── 指标正确性 ──

    [Fact]
    public void Sma_MatchesManualAverage()
    {
        var k = Synthetic(60);
        var (specs, _) = IndicatorEngine.Parse("MA5");
        var output = IndicatorEngine.Compute(specs[0], k)["MA5"];
        for (var i = 4; i < k.Length; i++)
        {
            var manual = Enumerable.Range(i - 4, 5).Select(j => k.Close[j]).Average();
            Assert.NotNull(output[i]);
            Assert.Equal(manual, output[i]!.Value, 10);
        }
        Assert.Null(output[3]); // 预热区
    }

    [Fact]
    public void Kdj_J_Identity()
    {
        var k = Synthetic(120);
        var (specs, _) = IndicatorEngine.Parse("KDJ");
        var output = IndicatorEngine.Compute(specs[0], k);
        for (var i = 0; i < k.Length; i++)
            if (output["KDJ_K"][i] is { } kv && output["KDJ_D"][i] is { } dv)
                Assert.Equal(3 * kv - 2 * dv, output["KDJ_J"][i]!.Value, 10);
        Assert.Contains(output["KDJ_J"], v => v.HasValue);
    }

    [Fact]
    public void Macd_HistEqualsDifMinusDea()
    {
        var k = Synthetic(200);
        var (specs, _) = IndicatorEngine.Parse("MACD");
        var output = IndicatorEngine.Compute(specs[0], k);
        for (var i = 0; i < k.Length; i++)
            if (output["MACD_DIF"][i] is { } dif && output["MACD_DEA"][i] is { } dea)
                Assert.Equal(dif - dea, output["MACD_HIST"][i]!.Value, 8);
    }

    [Fact]
    public void Boll_UpperAboveMiddleAboveLower()
    {
        var k = Synthetic(80);
        var (specs, _) = IndicatorEngine.Parse("BOLL");
        var output = IndicatorEngine.Compute(specs[0], k);
        for (var i = 0; i < k.Length; i++)
            if (output["BOLL_MIDDLE"][i] is { } mid)
            {
                Assert.True(output["BOLL_UPPER"][i] >= mid);
                Assert.True(mid >= output["BOLL_LOWER"][i]);
            }
    }

    [Fact]
    public void Lookback_PositiveForAllKinds()
    {
        var (specs, _) = IndicatorEngine.Parse("MA20,EMA20,RSI14,ATR14,CCI14,MACD,KDJ,BOLL");
        foreach (var s in specs)
            Assert.True(IndicatorEngine.Lookback(s) > 0, s.Key);
        var (obv, _) = IndicatorEngine.Parse("OBV");
        Assert.Equal(0, IndicatorEngine.Lookback(obv[0]));
    }

    [Fact]
    public void PatternEngine_DetectsOnSyntheticDoji()
    {
        // 构造一根标准十字星：开收几乎相等、上下影线长
        var k = Synthetic(40);
        var i = 35;
        k.Open[i] = 10.0; k.Close[i] = 10.001; k.High[i] = 10.8; k.Low[i] = 9.2;
        var hits = PatternEngine.Detect(k, ["Doji"], fromIndex: 30);
        Assert.Contains(hits, h => h.Date == k.Dates[i] && h.Pattern == "Doji");
    }

    [Fact]
    public void KlineSeries_IndexOf()
    {
        var k = Synthetic(10);
        Assert.Equal(0, k.IndexOf("2023-12-31"));
        Assert.Equal(5, k.IndexOf(k.Dates[5]));
        Assert.Equal(10, k.IndexOf("2099-01-01"));
    }
}
