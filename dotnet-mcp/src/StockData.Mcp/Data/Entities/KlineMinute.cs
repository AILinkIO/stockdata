using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data.Entities;

/// <summary>
/// 分钟 K 线（表 kline_minute，移植 db/models/kline.py）。frequency = 5/15/30/60。
/// 旧库按 bar_time RANGE 年度分区（性能优化）；此处先建普通表，分区 DDL 后续可加。
/// </summary>
[PrimaryKey(nameof(Code), nameof(Frequency), nameof(BarTime))]
public class KlineMinute
{
    [MaxLength(12)] public string Code { get; set; } = "";
    public short Frequency { get; set; }            // 5 / 15 / 30 / 60
    public DateTimeOffset BarTime { get; set; }

    [Precision(12, 4)] public decimal? Open { get; set; }
    [Precision(12, 4)] public decimal? High { get; set; }
    [Precision(12, 4)] public decimal? Low { get; set; }
    [Precision(12, 4)] public decimal? Close { get; set; }
    public long? Volume { get; set; }
    [Precision(20, 4)] public decimal? Amount { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}
