using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace StockData.Mcp.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddRemainingTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "adjust_factor",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    divid_operate_date = table.Column<DateOnly>(type: "date", nullable: false),
                    fore_adjust_factor = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    back_adjust_factor = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                    adjust_factor = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_adjust_factor", x => new { x.code, x.divid_operate_date });
                });

            migrationBuilder.CreateTable(
                name: "dividend",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    plan_announce_date = table.Column<DateOnly>(type: "date", nullable: false),
                    year_type = table.Column<string>(type: "character varying(7)", maxLength: 7, nullable: false),
                    year = table.Column<short>(type: "smallint", nullable: false),
                    regist_date = table.Column<DateOnly>(type: "date", nullable: true),
                    operate_date = table.Column<DateOnly>(type: "date", nullable: true),
                    pay_date = table.Column<DateOnly>(type: "date", nullable: true),
                    cash_ps_before_tax = table.Column<decimal>(type: "numeric(12,6)", precision: 12, scale: 6, nullable: true),
                    cash_ps_after_tax = table.Column<decimal>(type: "numeric(12,6)", precision: 12, scale: 6, nullable: true),
                    stocks_ps = table.Column<decimal>(type: "numeric(12,6)", precision: 12, scale: 6, nullable: true),
                    reserve_to_stock_ps = table.Column<decimal>(type: "numeric(12,6)", precision: 12, scale: 6, nullable: true),
                    detail = table.Column<string>(type: "jsonb", nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_dividend", x => new { x.code, x.plan_announce_date, x.year_type });
                });

            migrationBuilder.CreateTable(
                name: "financial_report",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    report_type = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    stat_date = table.Column<DateOnly>(type: "date", nullable: false),
                    pub_date = table.Column<DateOnly>(type: "date", nullable: true),
                    metrics = table.Column<string>(type: "jsonb", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_financial_report", x => new { x.code, x.report_type, x.stat_date });
                });

            migrationBuilder.CreateTable(
                name: "index_constituent",
                columns: table => new
                {
                    index_code = table.Column<string>(type: "character varying(8)", maxLength: 8, nullable: false),
                    snap_date = table.Column<DateOnly>(type: "date", nullable: false),
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    code_name = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_index_constituent", x => new { x.index_code, x.snap_date, x.code });
                });

            migrationBuilder.CreateTable(
                name: "stock_basic",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    code_name = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true),
                    ipo_date = table.Column<DateOnly>(type: "date", nullable: true),
                    out_date = table.Column<DateOnly>(type: "date", nullable: true),
                    type = table.Column<short>(type: "smallint", nullable: true),
                    status = table.Column<short>(type: "smallint", nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_stock_basic", x => x.code);
                });

            migrationBuilder.CreateTable(
                name: "stock_industry",
                columns: table => new
                {
                    snap_date = table.Column<DateOnly>(type: "date", nullable: false),
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    code_name = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true),
                    industry = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true),
                    industry_classification = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_stock_industry", x => new { x.snap_date, x.code });
                });

            migrationBuilder.CreateTable(
                name: "stock_list_snapshot",
                columns: table => new
                {
                    snap_date = table.Column<DateOnly>(type: "date", nullable: false),
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    code_name = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: true),
                    trade_status = table.Column<bool>(type: "boolean", nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_stock_list_snapshot", x => new { x.snap_date, x.code });
                });

            migrationBuilder.CreateTable(
                name: "trade_calendar",
                columns: table => new
                {
                    calendar_date = table.Column<DateOnly>(type: "date", nullable: false),
                    is_trading_day = table.Column<bool>(type: "boolean", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_trade_calendar", x => x.calendar_date);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "adjust_factor");

            migrationBuilder.DropTable(
                name: "dividend");

            migrationBuilder.DropTable(
                name: "financial_report");

            migrationBuilder.DropTable(
                name: "index_constituent");

            migrationBuilder.DropTable(
                name: "stock_basic");

            migrationBuilder.DropTable(
                name: "stock_industry");

            migrationBuilder.DropTable(
                name: "stock_list_snapshot");

            migrationBuilder.DropTable(
                name: "trade_calendar");
        }
    }
}
