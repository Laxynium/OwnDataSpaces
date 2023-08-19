using DotNet.Testcontainers.Containers;

namespace OwnDataSpaces.SqlServer.Tests.Fixtures;

public class TryToUseExistingContainer<T> : IAsyncLifetime
    where T : DockerContainer
{
    private readonly string _envVariableName;
    private readonly string _defaultConnectionString;
    private readonly Func<T> _containerFactory;
    private readonly Func<T, string> _containerConnectionString;
    private T? _container;

    public TryToUseExistingContainer(string envVariableName,
        string defaultConnectionString,
        Func<T> containerFactory,
        Func<T, string> containerConnectionString)
    {
        _envVariableName = envVariableName;
        _defaultConnectionString = defaultConnectionString;
        _containerFactory = containerFactory;
        _containerConnectionString = containerConnectionString;
    }

    public string GetConnectionString()
    {
        if (_container is null)
        {
            return _defaultConnectionString;
        }

        var connectionString = _containerConnectionString.Invoke(_container);

        return connectionString;
    }

    public async Task InitializeAsync()
    {
        if (UseExisting())
        {
            return;
        }

        _container = _containerFactory();
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (UseExisting())
        {
            return;
        }

        if (_container is not null)
        {
            await _container.StopAsync();
        }
    }

    private bool UseExisting()
    {
        return Environment.GetEnvironmentVariable(_envVariableName) == "true";
    }
}