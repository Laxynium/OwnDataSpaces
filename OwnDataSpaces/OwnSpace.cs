using Microsoft.Extensions.DependencyInjection;
using OwnDataSpaces.Internal;

namespace OwnDataSpaces;

public class OwnSpace
{
    private readonly Guid _spaceId = Guid.NewGuid();

    private readonly Func<HttpClient> _httpClientFactory;
    private readonly Func<IServiceProvider> _serviceProviderFactory;

    public OwnSpace(Func<HttpClient> httpClientFactory, Func<IServiceProvider> serviceProviderFactory)
    {
        _httpClientFactory = httpClientFactory;
        _serviceProviderFactory = serviceProviderFactory;
    }

    public Guid Id => _spaceId;

    public HttpClient GetClient()
    {
        var httpClient = _httpClientFactory();
        httpClient.DefaultRequestHeaders.Add("OwnSpaceId", _spaceId.ToString());
        return httpClient;
    }

    public AsyncServiceScope GetAsyncScope()
    {
        var serviceProvider = _serviceProviderFactory();
        var scope = serviceProvider.CreateAsyncScope();
        
        SetSpaceId(scope);

        return scope;
    }

    public IServiceScope GetScope()
    {
        var serviceProvider = _serviceProviderFactory();
        var scope = serviceProvider.CreateScope();
        
        SetSpaceId(scope);

        return scope;
    }

    private void SetSpaceId(IServiceScope scope)
    {
        var idProvider = scope.ServiceProvider.GetRequiredService<OwnSpaceProvider>();
        idProvider.SetSpaceId(_spaceId);
    }
}