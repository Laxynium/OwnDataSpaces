﻿using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OwnDataSpaces.EFCore;
using OwnDataSpaces.Internal;
using OwnDataSpaces.Logging;
using OwnDataSpaces.Postgres;
using OwnDataSpaces.SqlServer;

namespace OwnDataSpaces.Configuration;

public static class Extensions
{
    public static IServiceCollection AddOwnSpaces(this IServiceCollection services, Action<OwnSpacesOptions> configure)
    {
        var options = new OwnSpacesOptions { Type = "SqlServer" };
        configure(options);

        services.AddHttpContextAccessor();

        services.TryAddScoped<OwnSpaceProvider>();
        services.TryAddScoped<DatabaseOwnSpaceSetter>();

        services.TryAddScoped<SetOwnSpaceInterceptor>(sp =>
            new SetOwnSpaceInterceptor(sp.GetRequiredService<DatabaseOwnSpaceSetter>()));

        services.TryAddScoped<SetOwnSpaceSqlConnection>(sp =>
            new SetOwnSpaceSqlConnection(sp.GetRequiredService<DatabaseOwnSpaceSetter>()));

        if (options.Type == "SqlServer")
        {
            services.TryAddScoped<SetOwnSpaceSqlProvider>(_ => SqlServerOwnSpaceConfigurator.SetOwnSpaceSql);
        }

        if (options.Type == "Postgres")
        {
            services.TryAddScoped<SetOwnSpaceSqlProvider>(_ => PostgresOwnSpaceConfigurator.SetOwnSpaceSql);
        }

        services.AddTransient<SetupOwnSpaceLoggingContextMiddleware>();
        services.AddTransient<IStartupFilter, SetupOwnSpaceLoggingContextMiddlewareStartupFilter>();
        
        return services;
    }
}