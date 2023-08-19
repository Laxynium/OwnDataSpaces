using Microsoft.EntityFrameworkCore;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.SqlServer.Api;
using OwnDataSpaces.SqlServer.Tests.Utils;
using Testcontainers.MsSql;
using Xunit.Abstractions;

namespace OwnDataSpaces.SqlServer.Tests.Fixtures;

public class SqlServerFixture : IAsyncLifetime
{
    private readonly IMessageSink _sink;
    private TryToUseExistingContainer<MsSqlContainer>? _msSqlContainer;

    public SqlServerFixture(IMessageSink sink)
    {
        _sink = sink;
    }

    public string ConnectionString { get; set; } = null!;

    public async Task InitializeAsync()
    {
        await _sink.Measure("Start container", async () =>
        {
            _msSqlContainer = new TryToUseExistingContainer<MsSqlContainer>(
                "TESTCONTAINERS_USE_EXISTING_MSSQL",
                "Server=127.0.0.1;Database=Smart.Tests;User Id=sa;Password=Password123!@;TrustServerCertificate=True;",
                () => new MsSqlBuilder()
                    .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                    .WithPassword("Password123!@")
                    .Build(),
                c => c.GetConnectionString());

            await _msSqlContainer.InitializeAsync();

            ConnectionString = GetConnectionString(_msSqlContainer.GetConnectionString());
        });

        await _sink.Measure("Apply migrations", () => SetupDb(ConnectionString));

        await _sink.Measure("Apply data own spaces", () =>
        {
            var filter = TableFilters.NameContains("Migrations").Not();
            return SqlServerOwnSpaceConfigurator.Apply(ConnectionString, filter);
        });
    }

    private static string GetConnectionString(string cs)
    {
        var options = cs.Split(";").Where(x => !x.StartsWith("Database="));
        return string.Join(";", options.Append("Database=Smart.Tests"));
    }

    public async Task DisposeAsync()
    {
        if (_msSqlContainer is not null)
        {
            await _msSqlContainer.DisposeAsync();
        }
    }

    private async Task SetupDb(string connectionString)
    {
        var dbContextOptionsBuilder = new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(connectionString);
        await using var context = new AppDbContext(dbContextOptionsBuilder.Options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.MigrateAsync();
    }
}