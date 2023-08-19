using OwnDataSpaces.SqlServer.Tests.Utils;
using Xunit.Abstractions;

namespace OwnDataSpaces.SqlServer.Tests.Fixtures;

[CollectionDefinition(nameof(IntegrationTestFixtureCollection), DisableParallelization = false)]
public class IntegrationTestFixtureCollection : ICollectionFixture<IntegrationTestFixture>
{
}

public class IntegrationTestFixture : IAsyncLifetime
{
    private readonly IMessageSink _sink;
    private SqlServerFixture SqlServer { get; set; } = null!;
    public ApiFactoryFixture Api { get; private set; } = null!;

    public IntegrationTestFixture(IMessageSink sink)
    {
        _sink = sink;
    }

    public async Task InitializeAsync()
    {
        await _sink.Measure("Init SqlServer",() =>
        {
            SqlServer = new SqlServerFixture(_sink);
            return SqlServer.InitializeAsync();
        });

        _sink.Measure("Init ApiFactory", () =>
        {
            Api = new ApiFactoryFixture(SqlServer.ConnectionString);
            //triggers a creation of server
            _ = Api.Services;
        });
    }

    public async Task DisposeAsync()
    {
        await Api.DisposeAsync();
        await SqlServer.DisposeAsync();
    }
}