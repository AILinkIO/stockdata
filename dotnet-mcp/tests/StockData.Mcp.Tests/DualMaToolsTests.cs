using StockData.Mcp.Indicators;
using StockData.Mcp.Tools;

namespace StockData.Mcp.Tests;

public class DualMaCrossTests
{
    [Fact]
    public void 上穿为金叉()
        => Assert.Equal("金叉", DualMaTools.Cross(9.8, 10.0, 10.2, 10.0));

    [Fact]
    public void 下穿为死叉()
        => Assert.Equal("死叉", DualMaTools.Cross(10.2, 10.0, 9.8, 10.0));

    [Fact]
    public void 前一根粘合后上穿仍为金叉()
        => Assert.Equal("金叉", DualMaTools.Cross(10.0, 10.0, 10.2, 10.0));

    [Fact]
    public void 持续多头排列无信号()
        => Assert.Null(DualMaTools.Cross(10.2, 10.0, 10.3, 10.0));

    [Fact]
    public void 持续空头排列无信号()
        => Assert.Null(DualMaTools.Cross(9.8, 10.0, 9.7, 10.0));

    [Fact]
    public void warmup区不判定()
    {
        Assert.Null(DualMaTools.Cross(null, 10.0, 10.2, 10.0));
        Assert.Null(DualMaTools.Cross(9.8, null, 10.2, 10.0));
        Assert.Null(DualMaTools.Cross(9.8, 10.0, null, 10.0));
        Assert.Null(DualMaTools.Cross(9.8, 10.0, 10.2, null));
    }
}

public class DualMaTrendTests
{
    [Fact]
    public void 快线在上为多头排列()
        => Assert.Equal("多头排列", DualMaTools.Trend(10.2, 10.0));

    [Fact]
    public void 快线在下为空头排列()
        => Assert.Equal("空头排列", DualMaTools.Trend(9.8, 10.0));

    [Fact]
    public void 相等为均线粘合()
        => Assert.Equal("均线粘合", DualMaTools.Trend(10.0, 10.0));

    [Fact]
    public void warmup区不判定()
    {
        Assert.Null(DualMaTools.Trend(null, 10.0));
        Assert.Null(DualMaTools.Trend(10.0, null));
    }
}

public class DualMaEmaIntegrationTests
{
    /// <summary>
    /// 合成行情：长期下跌后 V 型反转持续上涨，
    /// EMA5 必然在某处上穿 EMA10，且全程恰好一次金叉、无死叉。
    /// </summary>
    [Fact]
    public void 合成V型反转产生唯一金叉()
    {
        var close = new double[60];
        for (var i = 0; i < 30; i++) close[i] = 100 - i;        // 下跌段
        for (var i = 30; i < 60; i++) close[i] = 70 + (i - 30) * 2; // 反转上涨段

        var fast = TalibComputer.Ema(close, 5);
        var slow = TalibComputer.Ema(close, 10);

        var signals = new List<string>();
        for (var i = 1; i < close.Length; i++)
        {
            var s = DualMaTools.Cross(fast[i - 1], slow[i - 1], fast[i], slow[i]);
            if (s is not null) signals.Add(s);
        }

        Assert.Equal(["金叉"], signals);
    }

    [Fact]
    public void EMA_warmup区为null其后有值()
    {
        var close = Enumerable.Range(1, 20).Select(i => (double)i).ToArray();
        var ema = TalibComputer.Ema(close, 10);
        var lookback = TalibComputer.EmaLookback(10);

        for (var i = 0; i < lookback; i++) Assert.Null(ema[i]);
        for (var i = lookback; i < close.Length; i++) Assert.NotNull(ema[i]);
    }
}
