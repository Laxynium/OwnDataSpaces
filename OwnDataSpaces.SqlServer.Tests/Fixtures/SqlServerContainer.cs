using Testcontainers.MsSql;

namespace OwnDataSpaces.SqlServer.Tests.Fixtures;

public class SqlServerContainer : IAsyncDisposable
{
    private readonly string _dbName;
    private readonly TryToUseExistingContainer<MsSqlContainer> _msSqlContainer;
    public string ConnectionString { get; set; } = string.Empty;


    public SqlServerContainer(string dbName)
    {
        _dbName = dbName;
        _msSqlContainer = new TryToUseExistingContainer<MsSqlContainer>(
            "TESTCONTAINERS_USE_EXISTING_MSSQL",
            $"Server=127.0.0.1;Database={dbName};User Id=sa;Password=Password123!@;TrustServerCertificate=True;",
            () => new MsSqlBuilder()
                .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
                .WithPassword("Password123!@")
                .Build(),
            c => c.GetConnectionString());
    }

    public async Task InitializeAsync()
    {
        await _msSqlContainer.InitializeAsync();
        ConnectionString = ReplaceDatabase(_msSqlContainer.GetConnectionString(), _dbName);
    }

    public async ValueTask DisposeAsync() => await _msSqlContainer.DisposeAsync();

    public static string ReplaceDatabase(string cs, string dbName)
    {
        var options = cs.Split(";").Where(x => !x.StartsWith("Database="));
        return string.Join(";", options.Append($"Database={dbName}"));
    }
}