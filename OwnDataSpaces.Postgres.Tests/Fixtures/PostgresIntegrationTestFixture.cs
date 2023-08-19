using OwnDataSpaces.Postgres.Tests.Utils;
using Xunit.Abstractions;

namespace OwnDataSpaces.Postgres.Tests.Fixtures;

[CollectionDefinition(nameof(PostgresIntegrationTestFixtureCollection), DisableParallelization = false)]
public class PostgresIntegrationTestFixtureCollection : ICollectionFixture<PostgresIntegrationTestFixture>
{
}

public class PostgresIntegrationTestFixture : IAsyncLifetime
{
    private readonly IMessageSink _sink;
    private PostgresFixture Database { get; set; } = null!;
    public ApiFactoryFixture Api { get; private set; } = null!;

    public PostgresIntegrationTestFixture(IMessageSink sink)
    {
        _sink = sink;
    }

    public async Task InitializeAsync()
    {
        await _sink.Measure("Init Postgres",() =>
        {
            Database = new PostgresFixture(_sink);
            return Database.InitializeAsync();
        });

        _sink.Measure("Init ApiFactory", () =>
        {
            Api = new ApiFactoryFixture(Database.ConnectionString);
            //triggers a creation of server
            _ = Api.Services;
        });
    }

    public async Task DisposeAsync()
    {
        await Api.DisposeAsync();
        await Database.DisposeAsync();
    }
}