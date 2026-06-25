using StockData.Mcp.Data;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>baostock 日线解析/映射——对齐 <c>writer.py</c> 的 _dec/_int/_date/_bool01 + _K_COL_MAP。</summary>
public class KlineParserTests
{
    [Fact]
    public void 空串与null转为null()
    {
        Assert.Null(KlineParser.Dec(""));
        Assert.Null(KlineParser.Dec(null));
        Assert.Null(KlineParser.Int(""));
        Assert.Null(KlineParser.Date(""));
        Assert.Null(KlineParser.Bool01(""));
    }

    [Fact]
    public void decimal_精确不经float()
    {
        // 0.1+0.2 在 double 下 != 0.3；decimal 字符串解析必须精确
        Assert.Equal(0.3m, KlineParser.Dec("0.1")!.Value + KlineParser.Dec("0.2")!.Value);
        Assert.Equal(10.2000m, KlineParser.Dec("10.2000"));
        Assert.Equal(123456789.123456m, KlineParser.Dec("123456789.123456"));
    }

    [Fact]
    public void 解析失败返回null不抛()
    {
        Assert.Null(KlineParser.Dec("abc"));
        Assert.Null(KlineParser.Int("3.5"));
        Assert.Null(KlineParser.Date("2026/06/11"));
    }

    [Fact]
    public void isST_只有1为真()
    {
        Assert.True(KlineParser.Bool01("1"));
        Assert.False(KlineParser.Bool01("0"));
        Assert.False(KlineParser.Bool01("2"));
    }

    [Fact]
    public void 列映射_日线全字段()
    {
        var rec = new Dictionary<string, string?>
        {
            ["date"] = "2024-01-02", ["code"] = "sh.600000",
            ["open"] = "10.2000", ["high"] = "10.5", ["low"] = "10.1", ["close"] = "10.4",
            ["preclose"] = "10.15", ["volume"] = "123456789", ["amount"] = "1300000000.5",
            ["turn"] = "0.523000", ["pctChg"] = "2.463054", ["tradestatus"] = "1", ["isST"] = "0",
            ["peTTM"] = "8.123456", ["pbMRQ"] = "0.9", ["psTTM"] = "1.2", ["pcfNcfTTM"] = "3.4",
        };
        var k = KlineParser.ToKline(rec, "sh.600000", "d");

        Assert.Equal("sh.600000", k.Code);
        Assert.Equal("d", k.Frequency);
        Assert.Equal(new DateOnly(2024, 1, 2), k.TradeDate);
        Assert.Equal(10.2000m, k.Open);
        Assert.Equal(123456789L, k.Volume);
        Assert.Equal((short)1, k.TradeStatus);
        Assert.False(k.IsSt);
        Assert.Equal(8.123456m, k.PeTtm);
    }

    [Fact]
    public void 周线缺估值列_按null()
    {
        // 周/月线 fields 不含 tradestatus/isST/估值四件套
        var rec = new Dictionary<string, string?>
        {
            ["date"] = "2024-01-05", ["open"] = "10", ["high"] = "11", ["low"] = "9", ["close"] = "10.5",
            ["volume"] = "1", ["amount"] = "2", ["turn"] = "3", ["pctChg"] = "4",
        };
        var k = KlineParser.ToKline(rec, "sh.600000", "w");
        Assert.Null(k.TradeStatus);
        Assert.Null(k.IsSt);
        Assert.Null(k.PeTtm);
        Assert.Null(k.Preclose);
    }

    [Fact]
    public void payload_转列表与最大日期()
    {
        var payload = new FetchPayload(
            new[] { "date", "code", "close" },
            new IReadOnlyList<string?>[]
            {
                new string?[] { "2024-01-02", "sh.600000", "10.4" },
                new string?[] { "2024-01-05", "sh.600000", "10.6" },
                new string?[] { "2024-01-03", "sh.600000", "10.5" },
            });

        var klines = KlineParser.ToKlines(payload, "sh.600000", "d");
        Assert.Equal(3, klines.Count);
        Assert.Equal(10.6m, klines[1].Close);
        Assert.Equal(new DateOnly(2024, 1, 5), KlineParser.MaxDate(payload));
    }

    [Fact]
    public void 空payload最大日期为null()
        => Assert.Null(KlineParser.MaxDate(FetchPayload.Empty));
}
