using MartinCostello.Logging.XUnit;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.EFCore;
using OwnDataSpaces.SqlServer.Api;
using Xunit.Abstractions;

namespace OwnDataSpaces.SqlServer.Tests.Fixtures;

public class ApiFactoryFixture : WebApplicationFactory<Program>, ITestOutputHelperAccessor
{
    private readonly string _connectionString;

    public ApiFactoryFixture(string connectionString)
    {
        _connectionString = connectionString;
    }

    public ITestOutputHelper? OutputHelper { get; set; }

    public OwnSpace CreateOwnSpace() =>
        new(() => CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false
        }), () => Services);

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureLogging(b =>
        {
            b.ClearProviders();
            b.AddXUnit(this);
        });
        builder.ConfigureAppConfiguration((c, b) =>
        {
            b.Sources.Clear();
            b.AddInMemoryCollection(new Dictionary<string, string?>
            {
                { "Logging:LogLevel:Default", "Information" },
                { "Logging:LogLevel:Microsoft.AspNetCore", "Warning" },
                { "Logging:LogLevel:Microsoft.EntityFrameworkCore.Database.Command", "Error"}
            });
        });
        builder.ConfigureTestServices((services) =>
        {
            services.AddOwnSpaces(opt => opt.UseSqlServer());

            services.RemoveAll(typeof(DbContextOptions<AppDbContext>));
            services.AddDbContext<AppDbContext>((sp, opt) =>
            {
                opt.UseSqlServer(_connectionString);
                opt.EnableDetailedErrors();
                opt.EnableSensitiveDataLogging();

                opt.AddInterceptors(sp.GetRequiredService<SetOwnSpaceInterceptor>());
            });
        });
    }
}