using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data.Entities;

// 宏观经济表（移植 db/models/macro.py）。利率/比率 numeric(8,4)、货币量 numeric(20,4)、
// 同比环比 numeric(10,4)。deposit/loan 的列名带数字（3month/6month/1year…），snake_case 约定
// 不可靠 → 显式 [Column]；rrr/money_supply 的 m0_month 等约定正确，无需 [Column]。

/// <summary>基准存款利率（表 deposit_rate）。</summary>
[PrimaryKey(nameof(PubDate))]
public class DepositRate
{
    public DateOnly PubDate { get; set; }
    [Precision(8, 4)] public decimal? DemandDepositRate { get; set; }
    [Column("fixed_deposit_rate_3month"), Precision(8, 4)] public decimal? FixedDepositRate3Month { get; set; }
    [Column("fixed_deposit_rate_6month"), Precision(8, 4)] public decimal? FixedDepositRate6Month { get; set; }
    [Column("fixed_deposit_rate_1year"), Precision(8, 4)] public decimal? FixedDepositRate1Year { get; set; }
    [Column("fixed_deposit_rate_2year"), Precision(8, 4)] public decimal? FixedDepositRate2Year { get; set; }
    [Column("fixed_deposit_rate_3year"), Precision(8, 4)] public decimal? FixedDepositRate3Year { get; set; }
    [Column("fixed_deposit_rate_5year"), Precision(8, 4)] public decimal? FixedDepositRate5Year { get; set; }
    [Column("installment_fixed_deposit_rate_1year"), Precision(8, 4)] public decimal? InstallmentFixedDepositRate1Year { get; set; }
    [Column("installment_fixed_deposit_rate_3year"), Precision(8, 4)] public decimal? InstallmentFixedDepositRate3Year { get; set; }
    [Column("installment_fixed_deposit_rate_5year"), Precision(8, 4)] public decimal? InstallmentFixedDepositRate5Year { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>基准贷款利率（表 loan_rate）。</summary>
[PrimaryKey(nameof(PubDate))]
public class LoanRate
{
    public DateOnly PubDate { get; set; }
    [Column("loan_rate_6month"), Precision(8, 4)] public decimal? LoanRate6Month { get; set; }
    [Column("loan_rate_6month_to_1year"), Precision(8, 4)] public decimal? LoanRate6MonthTo1Year { get; set; }
    [Column("loan_rate_1year_to_3year"), Precision(8, 4)] public decimal? LoanRate1YearTo3Year { get; set; }
    [Column("loan_rate_3year_to_5year"), Precision(8, 4)] public decimal? LoanRate3YearTo5Year { get; set; }
    [Column("loan_rate_above_5year"), Precision(8, 4)] public decimal? LoanRateAbove5Year { get; set; }
    [Column("mortgage_rate_below_5year"), Precision(8, 4)] public decimal? MortgageRateBelow5Year { get; set; }
    [Column("mortgage_rate_above_5year"), Precision(8, 4)] public decimal? MortgageRateAbove5Year { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>存款准备金率（表 required_reserve_ratio）。</summary>
[PrimaryKey(nameof(PubDate), nameof(EffectiveDate))]
public class RequiredReserveRatio
{
    public DateOnly PubDate { get; set; }
    public DateOnly EffectiveDate { get; set; }
    [Precision(8, 4)] public decimal? BigInstitutionsRatioPre { get; set; }
    [Precision(8, 4)] public decimal? BigInstitutionsRatioAfter { get; set; }
    [Precision(8, 4)] public decimal? MediumInstitutionsRatioPre { get; set; }
    [Precision(8, 4)] public decimal? MediumInstitutionsRatioAfter { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>月度货币供应量（表 money_supply_month）。</summary>
[PrimaryKey(nameof(StatYear), nameof(StatMonth))]
public class MoneySupplyMonth
{
    public short StatYear { get; set; }
    public short StatMonth { get; set; }
    [Precision(20, 4)] public decimal? M0Month { get; set; }
    [Precision(10, 4)] public decimal? M0Yoy { get; set; }
    [Precision(10, 4)] public decimal? M0ChainRelative { get; set; }
    [Precision(20, 4)] public decimal? M1Month { get; set; }
    [Precision(10, 4)] public decimal? M1Yoy { get; set; }
    [Precision(10, 4)] public decimal? M1ChainRelative { get; set; }
    [Precision(20, 4)] public decimal? M2Month { get; set; }
    [Precision(10, 4)] public decimal? M2Yoy { get; set; }
    [Precision(10, 4)] public decimal? M2ChainRelative { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>年度货币供应量（表 money_supply_year）。</summary>
[PrimaryKey(nameof(StatYear))]
public class MoneySupplyYear
{
    public short StatYear { get; set; }
    [Precision(20, 4)] public decimal? M0Year { get; set; }
    [Precision(10, 4)] public decimal? M0YearYoy { get; set; }
    [Precision(20, 4)] public decimal? M1Year { get; set; }
    [Precision(10, 4)] public decimal? M1YearYoy { get; set; }
    [Precision(20, 4)] public decimal? M2Year { get; set; }
    [Precision(10, 4)] public decimal? M2YearYoy { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}
