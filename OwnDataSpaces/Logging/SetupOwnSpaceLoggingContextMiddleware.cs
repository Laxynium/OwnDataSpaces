using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OwnDataSpaces.Internal;

namespace OwnDataSpaces.Logging;

internal class SetupOwnSpaceLoggingContextMiddleware : IMiddleware
{
    private readonly ILogger<SetupOwnSpaceLoggingContextMiddleware> _logger;

    public SetupOwnSpaceLoggingContextMiddleware(ILogger<SetupOwnSpaceLoggingContextMiddleware> logger)
    {
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        var ownSpaceProvider = context.RequestServices.GetRequiredService<OwnSpaceProvider>();
        var ownSpace = ownSpaceProvider.GetSpaceId();
        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            { Constants.ScopePropertyName, ownSpace }
        });

        await next(context);
    }
}