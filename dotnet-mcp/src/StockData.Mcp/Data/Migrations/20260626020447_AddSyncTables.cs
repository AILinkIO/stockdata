using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace StockData.Mcp.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddSyncTables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "stock_sync_task",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    kind = table.Column<string>(type: "character varying(8)", maxLength: 8, nullable: false),
                    status = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    datasets_done = table.Column<string[]>(type: "text[]", nullable: false),
                    requested_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    started_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    finished_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    error = table.Column<string>(type: "character varying(512)", maxLength: 512, nullable: true),
                    attempt = table.Column<int>(type: "integer", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_stock_sync_task", x => new { x.code, x.kind });
                });

            migrationBuilder.CreateTable(
                name: "synced_stock",
                columns: table => new
                {
                    code = table.Column<string>(type: "character varying(12)", maxLength: 12, nullable: false),
                    first_seen_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    minute_enabled = table.Column<bool>(type: "boolean", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_synced_stock", x => x.code);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "stock_sync_task");

            migrationBuilder.DropTable(
                name: "synced_stock");
        }
    }
}
