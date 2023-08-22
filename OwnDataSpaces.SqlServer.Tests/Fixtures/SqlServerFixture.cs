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
    private SqlServerContainer _msSqlContainer;

    public SqlServerFixture(IMessageSink sink)
    {
        _sink = sink;
    }

    public string ConnectionString => _msSqlContainer.ConnectionString;

    public async Task InitializeAsync()
    {
        await _sink.Measure("Start container", async () =>
        {
            _msSqlContainer = new SqlServerContainer("Smart.Tests");
            await _msSqlContainer.InitializeAsync();
        });

        await _sink.Measure("Apply migrations", () => SetupDb(ConnectionString));

        await _sink.Measure("Apply data own spaces", () =>
        {
            var filter = TableFilters.NameContains("Migrations").Not();
            return SqlServerOwnSpaceConfigurator.Apply(ConnectionString, filter);
        });
    }

    public async Task DisposeAsync()
    {
        await _msSqlContainer.DisposeAsync();
    }

    private async Task SetupDb(string connectionString)
    {
        var dbContextOptionsBuilder = new DbContextOptionsBuilder<AppDbContext>().UseSqlServer(connectionString);
        await using var context = new AppDbContext(dbContextOptionsBuilder.Options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.MigrateAsync();
    }
}