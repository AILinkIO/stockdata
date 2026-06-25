using StockData.Mcp.Data;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Tests;

public class CodeNormalizerTests
{
    [Theory]
    [InlineData("sh.600000", "sh.600000")]
    [InlineData("SH.600000", "sh.600000")]
    [InlineData(" sz.000001 ", "sz.000001")]
    [InlineData("600000.SH", "sh.600000")]
    [InlineData("000001.sz", "sz.000001")]
    [InlineData("600000", "sh.600000")]   // 6→上交所
    [InlineData("000001", "sz.000001")]   // 0→深交所
    [InlineData("300750", "sz.300750")]   // 3→深交所
    [InlineData("830799", "bj.830799")]   // 8→北交所
    public void 归一化为baostock形式(string input, string expected)
        => Assert.Equal(expected, CodeNormalizer.ToBaostock(input));

    [Fact]
    public void 未知形式原样返回()
        => Assert.Equal("XYZ", CodeNormalizer.ToBaostock("XYZ"));
}

public class KlineSerializeTests
{
    [Fact]
    public void 序列化_snake_case键_精度与null()
    {
        var rows = new List<Kline>
        {
            new()
            {
                Code = "sh.600000", Frequency = "d", TradeDate = new DateOnly(2024, 1, 2),
                Open = 10.2000m, Close = 10.5000m, Volume = 1000L, TradeStatus = 1, IsSt = false,
                PeTtm = 8.123456m, Preclose = null,
                UpdatedAt = new DateTimeOffset(2024, 1, 2, 8, 0, 0, TimeSpan.Zero),
            },
        };
        var json = KlineReadService.Serialize(rows);

        Assert.Contains("\"trade_date\":\"2024-01-02\"", json);
        Assert.Contains("\"close\":10.5000", json);     // decimal 精度保留
        Assert.Contains("\"volume\":1000", json);
        Assert.Contains("\"is_st\":false", json);
        Assert.Contains("\"preclose\":null", json);     // null 列保留
        Assert.Contains("\"pe_ttm\":8.123456", json);
    }
}
