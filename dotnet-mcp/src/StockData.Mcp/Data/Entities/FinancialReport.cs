using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data.Entities;

/// <summary>
/// 财报（表 <c>financial_report</c>）：八类共用单表 + report_type + JSONB metrics。
/// stat_date 报告期（主键），同报告期重披露时 pub_date/metrics 被 upsert 覆盖。
/// </summary>
[PrimaryKey(nameof(Code), nameof(ReportType), nameof(StatDate))]
public class FinancialReport
{
    [MaxLength(12)] public string Code { get; set; } = "";
    [MaxLength(20)] public string ReportType { get; set; } = "";  // ReportType 枚举
    public DateOnly StatDate { get; set; }                        // 报告期

    public DateOnly? PubDate { get; set; }                        // 披露日期
    [Column(TypeName = "jsonb")] public string Metrics { get; set; } = "{}";

    public DateTimeOffset UpdatedAt { get; set; }
}
