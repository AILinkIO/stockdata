using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// dotnet 数据属主的 EF Core 上下文（迁移属主 = EF Core Migrations，可重建库，TASK D-D）。
///
/// 列映射显式对齐 Python alembic 既有 DDL（snake_case 列名、PG 数值精度、character(1)、
/// timestamptz），保证 schema 与旧库逐列一致——重建后字段语义不变，仅换属主。
/// 批量 upsert 走 `ExecuteSqlRaw` 的 `ON CONFLICT`（TASK D-B），不经 SaveChanges。
/// </summary>
public class StockDataDbContext(DbContextOptions<StockDataDbContext> options) : DbContext(options)
{
    public DbSet<Kline> Klines => Set<Kline>();
    public DbSet<KlineMinute> KlineMinutes => Set<KlineMinute>();
    public DbSet<DataWatermark> DataWatermarks => Set<DataWatermark>();
    public DbSet<AdjustFactor> AdjustFactors => Set<AdjustFactor>();
    public DbSet<Dividend> Dividends => Set<Dividend>();
    public DbSet<FinancialReport> FinancialReports => Set<FinancialReport>();
    public DbSet<StockBasic> StockBasics => Set<StockBasic>();
    public DbSet<TradeCalendar> TradeCalendars => Set<TradeCalendar>();
    public DbSet<StockListSnapshot> StockListSnapshots => Set<StockListSnapshot>();
    public DbSet<IndexConstituent> IndexConstituents => Set<IndexConstituent>();
    public DbSet<StockIndustry> StockIndustries => Set<StockIndustry>();
    public DbSet<DepositRate> DepositRates => Set<DepositRate>();
    public DbSet<LoanRate> LoanRates => Set<LoanRate>();
    public DbSet<RequiredReserveRatio> RequiredReserveRatios => Set<RequiredReserveRatio>();
    public DbSet<MoneySupplyMonth> MoneySupplyMonths => Set<MoneySupplyMonth>();
    public DbSet<MoneySupplyYear> MoneySupplyYears => Set<MoneySupplyYear>();
    public DbSet<SyncedStock> SyncedStocks => Set<SyncedStock>();
    public DbSet<StockSyncTask> StockSyncTasks => Set<StockSyncTask>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<Kline>(e =>
        {
            e.ToTable("kline");
            e.HasKey(k => new { k.Code, k.Frequency, k.TradeDate });

            e.Property(k => k.Code).HasColumnName("code").HasColumnType("character varying(12)").IsRequired();
            e.Property(k => k.Frequency).HasColumnName("frequency").HasColumnType("character(1)").IsRequired();
            e.Property(k => k.TradeDate).HasColumnName("trade_date").HasColumnType("date").IsRequired();

            e.Property(k => k.Open).HasColumnName("open").HasColumnType("numeric(12,4)");
            e.Property(k => k.High).HasColumnName("high").HasColumnType("numeric(12,4)");
            e.Property(k => k.Low).HasColumnName("low").HasColumnType("numeric(12,4)");
            e.Property(k => k.Close).HasColumnName("close").HasColumnType("numeric(12,4)");
            e.Property(k => k.Preclose).HasColumnName("preclose").HasColumnType("numeric(12,4)");
            e.Property(k => k.Volume).HasColumnName("volume").HasColumnType("bigint");
            e.Property(k => k.Amount).HasColumnName("amount").HasColumnType("numeric(20,4)");
            e.Property(k => k.Turn).HasColumnName("turn").HasColumnType("numeric(10,6)");
            e.Property(k => k.PctChg).HasColumnName("pct_chg").HasColumnType("numeric(10,6)");
            e.Property(k => k.TradeStatus).HasColumnName("trade_status").HasColumnType("smallint");
            e.Property(k => k.IsSt).HasColumnName("is_st").HasColumnType("boolean");
            e.Property(k => k.PeTtm).HasColumnName("pe_ttm").HasColumnType("numeric(14,6)");
            e.Property(k => k.PbMrq).HasColumnName("pb_mrq").HasColumnType("numeric(14,6)");
            e.Property(k => k.PsTtm).HasColumnName("ps_ttm").HasColumnType("numeric(14,6)");
            e.Property(k => k.PcfNcfTtm).HasColumnName("pcf_ncf_ttm").HasColumnType("numeric(14,6)");

            e.Property(k => k.UpdatedAt).HasColumnName("updated_at")
                .HasColumnType("timestamp with time zone")
                .HasDefaultValueSql("now()").IsRequired();
        });

        b.Entity<DataWatermark>(e =>
        {
            e.ToTable("data_watermark");
            e.HasKey(w => new { w.Code, w.DataType });

            e.Property(w => w.Code).HasColumnName("code").HasColumnType("character varying(12)")
                .HasDefaultValue("").IsRequired();
            e.Property(w => w.DataType).HasColumnName("data_type").HasColumnType("character varying(24)").IsRequired();
            e.Property(w => w.FirstDate).HasColumnName("first_date").HasColumnType("date");
            e.Property(w => w.LastDate).HasColumnName("last_date").HasColumnType("date").IsRequired();
            e.Property(w => w.LastFetchedAt).HasColumnName("last_fetched_at")
                .HasColumnType("timestamp with time zone").IsRequired();
        });

        // 属性名 AdjustFactorValue ≠ 列名（列名 adjust_factor，与类名同不可作属性名）
        b.Entity<AdjustFactor>().Property(a => a.AdjustFactorValue).HasColumnName("adjust_factor");

        // 其余实体（P5 移植）：用 snake_case 约定自动映射表名/列名（避免逐列手写），
        // updated_at 统一默认 now()。已显式映射的列([Column]/Fluent，如带数字列名的宏观表)跳过，不覆盖。
        foreach (var entity in b.Model.GetEntityTypes())
        {
            entity.SetTableName(ToSnakeCase(entity.ClrType.Name));
            foreach (var prop in entity.GetProperties())
                if (prop.GetColumnName() == prop.Name)   // 未显式映射才套约定
                    prop.SetColumnName(ToSnakeCase(prop.Name));
            entity.FindProperty(nameof(Kline.UpdatedAt))?.SetDefaultValueSql("now()");
        }
    }

    private static string ToSnakeCase(string name)
        => Regex.Replace(name, "(?<=[a-z0-9])(?=[A-Z])", "_").ToLowerInvariant();
}
