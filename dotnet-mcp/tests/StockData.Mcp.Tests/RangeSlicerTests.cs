using StockData.Mcp.Data;

namespace StockData.Mcp.Tests;

/// <summary>切片黄金对照——逐 case 对齐 Python <c>test_readthrough_slicing.py</c>。</summary>
public class RangeSlicerTests
{
    private static DateOnly D(int y, int m, int d) => new(y, m, d);
    private static List<(DateOnly, DateOnly)> Slice(DateOnly fs, DateOnly fe, int? max)
        => RangeSlicer.Slice(fs, fe, max).ToList();

    [Fact]
    public void test_no_limit_returns_whole_range()
        => Assert.Equal(new[] { (D(1990, 12, 19), D(2026, 6, 11)) },
            Slice(D(1990, 12, 19), D(2026, 6, 11), null));

    [Fact]
    public void test_range_within_limit_is_single_slice()
        => Assert.Equal(new[] { (D(2026, 1, 1), D(2026, 1, 10)) },
            Slice(D(2026, 1, 1), D(2026, 1, 10), 30));

    [Fact]
    public void test_single_day_range()
        => Assert.Equal(new[] { (D(2026, 6, 11), D(2026, 6, 11)) },
            Slice(D(2026, 6, 11), D(2026, 6, 11), 10));

    [Fact]
    public void test_exact_multiple_of_limit()
        => Assert.Equal(new[]
            {
                (D(2026, 1, 1), D(2026, 1, 10)),
                (D(2026, 1, 11), D(2026, 1, 20)),
            },
            Slice(D(2026, 1, 1), D(2026, 1, 20), 10));

    [Fact]
    public void test_full_history_slices_are_contiguous_and_bounded()
    {
        var fs = D(1990, 12, 19);
        var fe = D(2026, 6, 11);
        const int maxDays = 3650;
        var slices = Slice(fs, fe, maxDays);

        Assert.Equal(fs, slices[0].Item1);
        Assert.Equal(fe, slices[^1].Item2);
        foreach (var (s, e) in slices)
        {
            Assert.True(s <= e);
            Assert.True(e.DayNumber - s.DayNumber + 1 <= maxDays);
        }
        for (var i = 1; i < slices.Count; i++)
            Assert.Equal(slices[i - 1].Item2.AddDays(1), slices[i].Item1);
    }

    [Fact]
    public void k_d_切片上限为3650_未列出返回null()
    {
        Assert.Equal(3650, RangeSlicer.SliceDays("k_d"));
        Assert.Equal(730, RangeSlicer.SliceDays("k_30"));
        Assert.Null(RangeSlicer.SliceDays("trade_calendar"));
    }
}
