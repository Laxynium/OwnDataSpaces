using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;

namespace OwnDataSpaces.Logging;

internal class SetupOwnSpaceLoggingContextMiddlewareStartupFilter : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) =>
        builder =>
        {
            builder.UseMiddleware<SetupOwnSpaceLoggingContextMiddleware>();
            next(builder);
        };
}