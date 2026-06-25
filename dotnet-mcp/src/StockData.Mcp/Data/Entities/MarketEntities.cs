using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data.Entities;

/// <summary>股票基本信息（表 <c>stock_basic</c>）。</summary>
[PrimaryKey(nameof(Code))]
public class StockBasic
{
    [MaxLength(12)] public string Code { get; set; } = "";
    [MaxLength(64)] public string? CodeName { get; set; }
    public DateOnly? IpoDate { get; set; }
    public DateOnly? OutDate { get; set; }
    public short? Type { get; set; }     // 1 股票 / 2 指数 / 3 其它
    public short? Status { get; set; }    // 1 上市 / 0 退市
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>交易日历（表 <c>trade_calendar</c>，全市场，无 code）。</summary>
[PrimaryKey(nameof(CalendarDate))]
public class TradeCalendar
{
    public DateOnly CalendarDate { get; set; }
    public bool IsTradingDay { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>股票列表快照（表 <c>stock_list_snapshot</c>）。</summary>
[PrimaryKey(nameof(SnapDate), nameof(Code))]
public class StockListSnapshot
{
    public DateOnly SnapDate { get; set; }
    [MaxLength(12)] public string Code { get; set; } = "";
    [MaxLength(64)] public string? CodeName { get; set; }
    public bool? TradeStatus { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>指数成分股（表 <c>index_constituent</c>，sz50/hs300/zz500）。</summary>
[PrimaryKey(nameof(IndexCode), nameof(SnapDate), nameof(Code))]
public class IndexConstituent
{
    [MaxLength(8)] public string IndexCode { get; set; } = "";
    public DateOnly SnapDate { get; set; }
    [MaxLength(12)] public string Code { get; set; } = "";
    [MaxLength(64)] public string? CodeName { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>行业分类（表 <c>stock_industry</c>）。</summary>
[PrimaryKey(nameof(SnapDate), nameof(Code))]
public class StockIndustry
{
    public DateOnly SnapDate { get; set; }
    [MaxLength(12)] public string Code { get; set; } = "";
    [MaxLength(64)] public string? CodeName { get; set; }
    [MaxLength(64)] public string? Industry { get; set; }
    [MaxLength(64)] public string? IndustryClassification { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}
