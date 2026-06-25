using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data.Entities;

/// <summary>除权因子（表 <c>adjust_factor</c>，移植自 db/models/adjust.py）。每个除权除息事件一行。</summary>
[PrimaryKey(nameof(Code), nameof(DividOperateDate))]
public class AdjustFactor
{
    [MaxLength(12)] public string Code { get; set; } = "";
    public DateOnly DividOperateDate { get; set; }

    [Precision(18, 8)] public decimal ForeAdjustFactor { get; set; }
    [Precision(18, 8)] public decimal BackAdjustFactor { get; set; }
    // 属性名不能与类名相同，列名在 DbContext 显式置为 adjust_factor
    [Precision(18, 8)] public decimal? AdjustFactorValue { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>分红送转（表 <c>dividend</c>）。关键日期/比例落列，低频字段进 detail(JSONB)。</summary>
[PrimaryKey(nameof(Code), nameof(PlanAnnounceDate), nameof(YearType))]
public class Dividend
{
    [MaxLength(12)] public string Code { get; set; } = "";
    public DateOnly PlanAnnounceDate { get; set; }
    [MaxLength(7)] public string YearType { get; set; } = "";   // report / operate

    public short Year { get; set; }
    public DateOnly? RegistDate { get; set; }
    public DateOnly? OperateDate { get; set; }
    public DateOnly? PayDate { get; set; }
    [Precision(12, 6)] public decimal? CashPsBeforeTax { get; set; }
    [Precision(12, 6)] public decimal? CashPsAfterTax { get; set; }
    [Precision(12, 6)] public decimal? StocksPs { get; set; }
    [Precision(12, 6)] public decimal? ReserveToStockPs { get; set; }
    [Column(TypeName = "jsonb")] public string? Detail { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}
