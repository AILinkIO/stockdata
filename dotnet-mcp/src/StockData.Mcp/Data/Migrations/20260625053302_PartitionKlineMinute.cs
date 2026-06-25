using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace StockData.Mcp.Data.Migrations
{
    /// <summary>
    /// kline_minute 普通表 → 按 bar_time 年度 RANGE 分区（对齐旧 alembic；EF 不建模分区，手写 DDL）。
    /// PK 含 bar_time（PG 要求分区键属于唯一约束）。保留现有数据，DEFAULT 分区兜底范围外。
    /// </summary>
    public partial class PartitionKlineMinute : Migration
    {
        private const string Cols =
            "code character varying(12) NOT NULL," +
            "frequency smallint NOT NULL," +
            "bar_time timestamp with time zone NOT NULL," +
            "open numeric(12,4),high numeric(12,4),low numeric(12,4),close numeric(12,4)," +
            "volume bigint,amount numeric(20,4)," +
            "updated_at timestamp with time zone NOT NULL DEFAULT now()," +
            "CONSTRAINT \"PK_kline_minute\" PRIMARY KEY (code,frequency,bar_time)";

        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("ALTER TABLE kline_minute RENAME TO kline_minute_plain;");
            // PK 索引名全局唯一：重命名旧约束，腾出 PK_kline_minute 给分区父表
            migrationBuilder.Sql("ALTER TABLE kline_minute_plain RENAME CONSTRAINT \"PK_kline_minute\" TO \"PK_kline_minute_plain\";");
            migrationBuilder.Sql($"CREATE TABLE kline_minute ({Cols}) PARTITION BY RANGE (bar_time);");
            for (var y = 2023; y <= 2031; y++)
                migrationBuilder.Sql(
                    $"CREATE TABLE kline_minute_{y} PARTITION OF kline_minute " +
                    $"FOR VALUES FROM ('{y}-01-01 00:00:00+00') TO ('{y + 1}-01-01 00:00:00+00');");
            migrationBuilder.Sql("CREATE TABLE kline_minute_default PARTITION OF kline_minute DEFAULT;");
            migrationBuilder.Sql("INSERT INTO kline_minute SELECT * FROM kline_minute_plain;");
            migrationBuilder.Sql("DROP TABLE kline_minute_plain;");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("ALTER TABLE kline_minute RENAME TO kline_minute_part;");
            migrationBuilder.Sql("ALTER TABLE kline_minute_part RENAME CONSTRAINT \"PK_kline_minute\" TO \"PK_kline_minute_part\";");
            migrationBuilder.Sql($"CREATE TABLE kline_minute ({Cols});");
            migrationBuilder.Sql("INSERT INTO kline_minute SELECT * FROM kline_minute_part;");
            migrationBuilder.Sql("DROP TABLE kline_minute_part;");   // 删分区父表自动删各分区
        }
    }
}
