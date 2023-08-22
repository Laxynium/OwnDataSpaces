using System.Data.Common;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.EFCore;
using OwnDataSpaces.SqlServer.Tests.Fixtures;

namespace OwnDataSpaces.SqlServer.Tests;

public class Database
{
    private readonly IServiceProvider _serviceProvider;
    public string ConnectionString { get; }
    public string SchemaName { get; }

    private Database(string connectionString, string schemaName, IServiceProvider serviceProvider)
    {
        ConnectionString = connectionString;
        SchemaName = schemaName;
        _serviceProvider = serviceProvider;
    }

    public static async Task<Database> CreateDatabase(string dbName)
    {
        var container = new SqlServerContainer(dbName);
        await container.InitializeAsync();
        await using (var masterConnection =
                     new SqlConnection(SqlServerContainer.ReplaceDatabase(container.ConnectionString, "master")))
        {
            await masterConnection.ExecuteAsync($"""
                                                 DROP DATABASE IF EXISTS [{dbName}];
                                                 CREATE DATABASE [{dbName}];                                                          
                                                 """);
        }

        var services = new ServiceCollection();
        services.AddLogging(l => l.ClearProviders());
        services.AddOwnSpaces(opt => opt.UseSqlServer());

        return new Database(container.ConnectionString, "dbo", services.BuildServiceProvider());
    }

    public async Task Execute(string sql)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.ExecuteAsync(sql);
    }

    public async Task EnsureOwnSpacesAreNotLeaking(string writeSql, string readSql, int expectedCount)
    {
        var ownSpace1 = new OwnSpace(() => new HttpClient(), () => _serviceProvider);
        await Write(ownSpace1, writeSql);

        var ownSpace2 = new OwnSpace(() => new HttpClient(), () => _serviceProvider);
        await Write(ownSpace2, writeSql);

        var ownSpace1ActualCount = await Read(ownSpace1, readSql);
        ownSpace1ActualCount.Should().Be(expectedCount);

        var ownSpace2ActualCount = await Read(ownSpace2, readSql);
        ownSpace2ActualCount.Should().Be(expectedCount);
    }

    private async Task Write(OwnSpace ownSpace, string sql)
    {
        await using var scope = ownSpace.GetAsyncScope();
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        var setOwnSpace = scope.ServiceProvider.GetRequiredService<SetOwnSpaceSqlConnection>();
        await setOwnSpace.SetOwnSpace(connection, default);

        await connection.ExecuteAsync(sql);
    }

    private async Task<int> Read(OwnSpace ownSpace, string sql)
    {
        await using var scope = ownSpace.GetAsyncScope();
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        var setOwnSpace = scope.ServiceProvider.GetRequiredService<SetOwnSpaceSqlConnection>();
        await setOwnSpace.SetOwnSpace(connection, default);

        return await connection.QuerySingleAsync<int>(sql);
    }
}