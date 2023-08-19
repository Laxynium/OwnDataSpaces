using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SmartIntegrationTests.SqlServer.Api.Migrations
{
    /// <inheritdoc />
    public partial class AddUniqueConstraint2 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Part1",
                schema: "Smart",
                table: "Orders",
                type: "nvarchar(450)",
                nullable: false,
                defaultValue: "");

            migrationBuilder.AddColumn<string>(
                name: "Part2",
                schema: "Smart",
                table: "Orders",
                type: "nvarchar(450)",
                nullable: false,
                defaultValue: "");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_Part1_Part2",
                schema: "Smart",
                table: "Orders",
                columns: new[] { "Part1", "Part2" },
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_Orders_Part1_Part2",
                schema: "Smart",
                table: "Orders");

            migrationBuilder.DropColumn(
                name: "Part1",
                schema: "Smart",
                table: "Orders");

            migrationBuilder.DropColumn(
                name: "Part2",
                schema: "Smart",
                table: "Orders");
        }
    }
}
