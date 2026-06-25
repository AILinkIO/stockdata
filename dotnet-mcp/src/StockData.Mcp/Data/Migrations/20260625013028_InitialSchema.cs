using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace StockData.Mcp.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "data_watermark",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", nullable: false, defaultValue: ""),
                    data_type = table.Column<string>(type: "character varying(24)", nullable: false),
                    first_date = table.Column<DateOnly>(type: "date", nullable: true),
                    last_date = table.Column<DateOnly>(type: "date", nullable: false),
                    last_fetched_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_data_watermark", x => new { x.code, x.data_type });
                });

            migrationBuilder.CreateTable(
                name: "kline",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", nullable: false),
                    frequency = table.Column<string>(type: "character(1)", nullable: false),
                    trade_date = table.Column<DateOnly>(type: "date", nullable: false),
                    open = table.Column<decimal>(type: "numeric(12,4)", nullable: true),
                    high = table.Column<decimal>(type: "numeric(12,4)", nullable: true),
                    low = table.Column<decimal>(type: "numeric(12,4)", nullable: true),
                    close = table.Column<decimal>(type: "numeric(12,4)", nullable: true),
                    preclose = table.Column<decimal>(type: "numeric(12,4)", nullable: true),
                    volume = table.Column<long>(type: "bigint", nullable: true),
                    amount = table.Column<decimal>(type: "numeric(20,4)", nullable: true),
                    turn = table.Column<decimal>(type: "numeric(10,6)", nullable: true),
                    pct_chg = table.Column<decimal>(type: "numeric(10,6)", nullable: true),
                    trade_status = table.Column<short>(type: "smallint", nullable: true),
                    is_st = table.Column<bool>(type: "boolean", nullable: true),
                    pe_ttm = table.Column<decimal>(type: "numeric(14,6)", nullable: true),
                    pb_mrq = table.Column<decimal>(type: "numeric(14,6)", nullable: true),
                    ps_ttm = table.Column<decimal>(type: "numeric(14,6)", nullable: true),
                    pcf_ncf_ttm = table.Column<decimal>(type: "numeric(14,6)", nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_kline", x => new { x.code, x.frequency, x.trade_date });
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "data_watermark");

            migrationBuilder.DropTable(
                name: "kline");
        }
    }
}
