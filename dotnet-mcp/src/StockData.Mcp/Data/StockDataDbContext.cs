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
    public DbSet<DataWatermark> DataWatermarks => Set<DataWatermark>();

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
    }
}
