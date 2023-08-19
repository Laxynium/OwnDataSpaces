using Microsoft.EntityFrameworkCore;
using Npgsql;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.Postgres.Api;
using OwnDataSpaces.Postgres.Tests.Utils;
using Testcontainers.PostgreSql;
using Xunit.Abstractions;

namespace OwnDataSpaces.Postgres.Tests.Fixtures;

public class PostgresFixture
{
    private const string AdminUserName = "postgres";
    private const string AdminPassword = "Password123!@";

    private const string AppUserName = "apptests";
    private const string AppPassword = "Password123!@";

    private const string DbName = "smart_tests";

    private static readonly string ExistingContainerConnectionString = new NpgsqlConnectionStringBuilder(string.Empty)
    {
        Host = "127.0.0.1",
        Database = DbName,
        Username = AdminUserName,
        Passfile = AdminPassword,
    }.ToString();

    private readonly IMessageSink _sink;
    private TryToUseExistingContainer<PostgreSqlContainer>? _container;

    public PostgresFixture(IMessageSink sink)
    {
        _sink = sink;
    }

    public string ConnectionString { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _container = await _sink.Measure("Start container", CreateInitializedContainer);
        var connectionString = _container.GetConnectionString();

        await _sink.Measure("Apply migrations", () => SetupDb(connectionString));

        await _sink.Measure("Apply data own spaces", () => PostgresOwnSpaceConfigurator.Apply(
            connectionString,
            TableFilters.NameContains("Migrations").Not(),
            AppUserName,
            AppPassword));

        ConnectionString = UseAppCredentials(connectionString);
    }

    private static async Task<TryToUseExistingContainer<PostgreSqlContainer>> CreateInitializedContainer()
    {
        var container = new TryToUseExistingContainer<PostgreSqlContainer>(
            "TESTCONTAINERS_USE_EXISTING_POSTGRES",
            ExistingContainerConnectionString,
            () => new PostgreSqlBuilder()
                .WithImage("postgres:15.3")
                .WithUsername(AdminUserName)
                .WithPassword(AdminPassword)
                .WithDatabase(DbName)
                .Build(),
            c => c.GetConnectionString());

        await container.InitializeAsync();

        return container;
    }

    private static string UseAppCredentials(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString)
        {
            Username = AppUserName,
            Password = AppPassword
        }.ToString();

    private static async Task SetupDb(string connectionString)
    {
        var dbContextOptionsBuilder = new DbContextOptionsBuilder<AppDbContext>().UseNpgsql(connectionString);
        await using var context = new AppDbContext(dbContextOptionsBuilder.Options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }
}