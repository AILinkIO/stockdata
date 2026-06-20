using StockData.Mcp.Indicators;
using StockData.Mcp.Tools;

namespace StockData.Mcp.Tests;

public class MaAlignmentTests
{
    [Fact]
    public void 严格递减为多头排列()
        => Assert.Equal("多头排列", MaAlignmentTools.Alignment([5.0, 4.0, 3.0, 2.0]));

    [Fact]
    public void 严格递增为空头排列()
        => Assert.Equal("空头排列", MaAlignmentTools.Alignment([2.0, 3.0, 4.0, 5.0]));

    [Fact]
    public void 升降混合为未排列()
        => Assert.Equal("未排列", MaAlignmentTools.Alignment([5.0, 3.0, 4.0, 2.0]));

    [Fact]
    public void 相等不构成严格递减()
        => Assert.Equal("未排列", MaAlignmentTools.Alignment([5.0, 5.0, 3.0, 2.0]));

    [Fact]
    public void 相等不构成严格递增()
        => Assert.Equal("未排列", MaAlignmentTools.Alignment([2.0, 3.0, 3.0, 5.0]));

    [Fact]
    public void 含null为warmup区不判定()
    {
        Assert.Null(MaAlignmentTools.Alignment([null, 4.0, 3.0, 2.0]));
        Assert.Null(MaAlignmentTools.Alignment([5.0, null, 3.0, 2.0]));
        Assert.Null(MaAlignmentTools.Alignment([5.0, 4.0, 3.0, null]));
    }

    [Fact]
    public void 两周期最小配置可判定()
    {
        Assert.Equal("多头排列", MaAlignmentTools.Alignment([10.0, 9.0]));
        Assert.Equal("空头排列", MaAlignmentTools.Alignment([9.0, 10.0]));
        Assert.Equal("未排列", MaAlignmentTools.Alignment([10.0, 10.0]));
    }
}

public class MaAlignmentSignalTests
{
    [Fact]
    public void 未排列转为多头为多头形成()
        => Assert.Equal("多头形成",
            MaAlignmentTools.Signal([5.0, 3.0, 4.0, 2.0], [6.0, 5.0, 4.0, 3.0]));

    [Fact]
    public void 多头转为未排列为多头破坏()
        => Assert.Equal("多头破坏",
            MaAlignmentTools.Signal([6.0, 5.0, 4.0, 3.0], [5.0, 3.0, 4.0, 2.0]));

    [Fact]
    public void 未排列转为空头为空头形成()
        => Assert.Equal("空头形成",
            MaAlignmentTools.Signal([5.0, 3.0, 4.0, 2.0], [2.0, 3.0, 4.0, 5.0]));

    [Fact]
    public void 空头转为未排列为空头破坏()
        => Assert.Equal("空头破坏",
            MaAlignmentTools.Signal([2.0, 3.0, 4.0, 5.0], [5.0, 3.0, 4.0, 2.0]));

    [Fact]
    public void 持续多头排列无信号()
        => Assert.Null(MaAlignmentTools.Signal([6.0, 5.0, 4.0, 3.0], [6.1, 5.1, 4.1, 3.1]));

    [Fact]
    public void 持续空头排列无信号()
        => Assert.Null(MaAlignmentTools.Signal([2.0, 3.0, 4.0, 5.0], [1.9, 2.9, 3.9, 4.9]));

    [Fact]
    public void 双侧未排列无信号()
        => Assert.Null(MaAlignmentTools.Signal([5.0, 5.0, 5.0, 5.0], [5.0, 5.0, 5.0, 5.0]));

    [Fact]
    public void 前一日warmup不判定()
        => Assert.Null(MaAlignmentTools.Signal(
            [null, 4.0, 3.0, 2.0], [6.0, 5.0, 4.0, 3.0]));

    [Fact]
    public void 当日warmup不判定()
        => Assert.Null(MaAlignmentTools.Signal(
            [6.0, 5.0, 4.0, 3.0], [null, 5.0, 4.0, 3.0]));
}

public class MaAlignmentSmaIntegrationTests
{
    [Fact]
    public void SMA_warmup区为null其后有值()
    {
        var close = Enumerable.Range(1, 20).Select(i => (double)i).ToArray();
        var sma = TalibComputer.Sma(close, 10);
        var lookback = TalibComputer.SmaLookback(10);

        for (var i = 0; i < lookback; i++) Assert.Null(sma[i]);
        for (var i = lookback; i < close.Length; i++) Assert.NotNull(sma[i]);
    }

    /// <summary>
    /// 合成 V 型反转：长期下跌后强劲上涨，
    /// MA 必然在反转后形成多头排列，全程至少一次"多头形成"信号 + 持续多头日。
    /// </summary>
    [Fact]
    public void 合成V型反转产生多头形成()
    {
        var close = new double[60];
        for (var i = 0; i < 30; i++) close[i] = 100 - i;        // 下跌段
        for (var i = 30; i < 60; i++) close[i] = 70 + (i - 30) * 2; // 反转上涨段

        var ma5 = TalibComputer.Sma(close, 5);
        var ma10 = TalibComputer.Sma(close, 10);
        var ma20 = TalibComputer.Sma(close, 20);

        var signals = new List<string>();
        var bullDays = 0;
        for (var i = 1; i < close.Length; i++)
        {
            var prev = new double?[] { ma5[i - 1], ma10[i - 1], ma20[i - 1] };
            var curr = new double?[] { ma5[i], ma10[i], ma20[i] };
            var s = MaAlignmentTools.Signal(prev, curr);
            if (s is not null) signals.Add(s);
            if (MaAlignmentTools.Alignment(curr) == "多头排列") bullDays++;
        }

        Assert.Contains("多头形成", signals);
        Assert.True(bullDays > 0);
    }

    /// <summary>
    /// 合成倒 V 型反转：长期上涨后强劲下跌，
    /// MA 必然在反转后形成空头排列，全程至少一次"空头形成"信号 + 持续空头日。
    /// </summary>
    [Fact]
    public void 合成倒V型反转产生空头形成()
    {
        var close = new double[60];
        for (var i = 0; i < 30; i++) close[i] = 100 + i;       // 上涨段
        for (var i = 30; i < 60; i++) close[i] = 200 - (i - 30) * 2; // 反转下跌段

        var ma5 = TalibComputer.Sma(close, 5);
        var ma10 = TalibComputer.Sma(close, 10);
        var ma20 = TalibComputer.Sma(close, 20);

        var signals = new List<string>();
        var bearDays = 0;
        for (var i = 1; i < close.Length; i++)
        {
            var prev = new double?[] { ma5[i - 1], ma10[i - 1], ma20[i - 1] };
            var curr = new double?[] { ma5[i], ma10[i], ma20[i] };
            var s = MaAlignmentTools.Signal(prev, curr);
            if (s is not null) signals.Add(s);
            if (MaAlignmentTools.Alignment(curr) == "空头排列") bearDays++;
        }

        Assert.Contains("空头形成", signals);
        Assert.True(bearDays > 0);
    }
}