using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace StockData.Mcp.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddKlineMinute : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "kline_minute",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    frequency = table.Column<short>(type: "smallint", nullable: false),
                    bar_time = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    open = table.Column<decimal>(type: "numeric(12,4)", precision: 12, scale: 4, nullable: true),
                    high = table.Column<decimal>(type: "numeric(12,4)", precision: 12, scale: 4, nullable: true),
                    low = table.Column<decimal>(type: "numeric(12,4)", precision: 12, scale: 4, nullable: true),
                    close = table.Column<decimal>(type: "numeric(12,4)", precision: 12, scale: 4, nullable: true),
                    volume = table.Column<long>(type: "bigint", nullable: true),
                    amount = table.Column<decimal>(type: "numeric(20,4)", precision: 20, scale: 4, nullable: true),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_kline_minute", x => new { x.code, x.frequency, x.bar_time });
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "kline_minute");
        }
    }
}
