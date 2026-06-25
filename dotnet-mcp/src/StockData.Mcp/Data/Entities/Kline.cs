namespace StockData.Mcp.Data.Entities;

/// <summary>
/// 日/周/月 K 线（表 <c>kline</c>，移植自 <c>db/models/kline.py</c>）。
/// 只存不复权数据（设计原则 2：复权读时计算）。
/// 估值与状态字段（preclose/trade_status/is_st/pe_ttm…）仅日线有值，周/月为 NULL。
/// 主键 (code, frequency, trade_date)；列类型/精度在 <see cref="StockDataDbContext"/> 配置。
/// </summary>
public class Kline
{
    public string Code { get; set; } = "";
    public string Frequency { get; set; } = "";   // 'd' / 'w' / 'm'，character(1)
    public DateOnly TradeDate { get; set; }

    public decimal? Open { get; set; }
    public decimal? High { get; set; }
    public decimal? Low { get; set; }
    public decimal? Close { get; set; }
    public decimal? Preclose { get; set; }        // 仅日线
    public long? Volume { get; set; }
    public decimal? Amount { get; set; }
    public decimal? Turn { get; set; }
    public decimal? PctChg { get; set; }
    public short? TradeStatus { get; set; }        // 仅日线
    public bool? IsSt { get; set; }                // 仅日线
    public decimal? PeTtm { get; set; }            // 仅日线，估值四件套
    public decimal? PbMrq { get; set; }
    public decimal? PsTtm { get; set; }
    public decimal? PcfNcfTtm { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}
