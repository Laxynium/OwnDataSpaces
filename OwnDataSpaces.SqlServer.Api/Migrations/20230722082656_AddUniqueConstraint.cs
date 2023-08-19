using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SmartIntegrationTests.SqlServer.Api.Migrations
{
    /// <inheritdoc />
    public partial class AddUniqueConstraint : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Code",
                schema: "Smart",
                table: "Orders",
                type: "nvarchar(450)",
                nullable: false,
                defaultValue: "");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_Code",
                schema: "Smart",
                table: "Orders",
                column: "Code",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_Orders_Code",
                schema: "Smart",
                table: "Orders");

            migrationBuilder.DropColumn(
                name: "Code",
                schema: "Smart",
                table: "Orders");
        }
    }
}
