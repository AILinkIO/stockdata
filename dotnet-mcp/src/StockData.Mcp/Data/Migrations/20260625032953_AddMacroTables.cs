using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace StockData.Mcp.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddMacroTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "deposit_rate",
                columns: table => new
                {
                    pub_date = table.Column<DateOnly>(type: "date", nullable: false),
                    demand_deposit_rate = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    fixed_deposit_rate_3month = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    fixed_deposit_rate_6month = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    fixed_deposit_rate_1year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    fixed_deposit_rate_2year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    fixed_deposit_rate_3year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    fixed_deposit_rate_5year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    installment_fixed_deposit_rate_1year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    installment_fixed_deposit_rate_3year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    installment_fixed_deposit_rate_5year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_deposit_rate", x => x.pub_date);
                });

            migrationBuilder.CreateTable(
                name: "loan_rate",
                columns: table => new
                {
                    pub_date = table.Column<DateOnly>(type: "date", nullable: false),
                    loan_rate_6month = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    loan_rate_6month_to_1year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    loan_rate_1year_to_3year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    loan_rate_3year_to_5year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    loan_rate_above_5year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    mortgage_rate_below_5year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    mortgage_rate_above_5year = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_loan_rate", x => x.pub_date);
                });

            migrationBuilder.CreateTable(
                name: "money_supply_month",
                columns: table => new
                {
                    stat_year = table.Column<short>(type: "smallint", nullable: false),
                    stat_month = table.Column<short>(type: "smallint", nullable: false),
                    m0_month = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    m0_yoy = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m0_chain_relative = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m1_month = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    m1_yoy = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m1_chain_relative = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m2_month = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    m2_yoy = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m2_chain_relative = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_money_supply_month", x => new { x.stat_year, x.stat_month });
                });

            migrationBuilder.CreateTable(
                name: "money_supply_year",
                columns: table => new
                {
                    stat_year = table.Column<short>(type: "smallint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    m0_year = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    m0_year_yoy = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m1_year = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    m1_year_yoy = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    m2_year = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    m2_year_yoy = table.Column<decimal>(type: "numeric(10,4)", precision: 10, scale: 4, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_money_supply_year", x => x.stat_year);
                });

            migrationBuilder.CreateTable(
                name: "required_reserve_ratio",
                columns: table => new
                {
                    pub_date = table.Column<DateOnly>(type: "date", nullable: false),
                    effective_date = table.Column<DateOnly>(type: "date", nullable: false),
                    big_institutions_ratio_pre = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    big_institutions_ratio_after = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    medium_institutions_ratio_pre = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    medium_institutions_ratio_after = table.Column<decimal>(type: "numeric(8,4)", precision: 8, scale: 4, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_required_reserve_ratio", x => new { x.pub_date, x.effective_date });
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "deposit_rate");

            migrationBuilder.DropTable(
                name: "loan_rate");

            migrationBuilder.DropTable(
                name: "money_supply_month");

            migrationBuilder.DropTable(
                name: "money_supply_year");

            migrationBuilder.DropTable(
                name: "required_reserve_ratio");
        }
    }
}
