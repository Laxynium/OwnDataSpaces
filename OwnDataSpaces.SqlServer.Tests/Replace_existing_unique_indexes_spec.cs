using Dapper;
using Microsoft.Data.SqlClient;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.SqlServer.Tests.Fixtures;
using Xunit.Abstractions;

namespace OwnDataSpaces.SqlServer.Tests;

public class ReplaceExistingUniqueIndexesSpec
{
    private readonly ITestOutputHelper _testOutputHelper;

    public ReplaceExistingUniqueIndexesSpec(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task When_there_is_alternate_key()
    {
        var db = await Database.CreateDatabase(_testOutputHelper);

        await db.Execute(schema => $"""
                                    CREATE TABLE [{schema}].[Table1](
                                        Id INT IDENTITY(1,1) PRIMARY KEY,
                                        AltId UNIQUEIDENTIFIER NOT NULL,
                                        Col1 NVARCHAR(100) NOT NULL
                                        CONSTRAINT AK_Table1_AltId UNIQUE(AltId)
                                    );
                                    """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, TableFilters.SchemaEquals(db.SchemaName));
    }

    [Fact]
    public async Task When_there_is_cycle()
    {
        var db = await Database.CreateDatabase(_testOutputHelper);

        await db.Execute(schema => $"""
                                    CREATE TABLE [{schema}].[Table1](
                                        Id INT IDENTITY(1,1) PRIMARY KEY,
                                        AltId UNIQUEIDENTIFIER NOT NULL,
                                        Table2_Id INT NOT NULL,
                                        Table2_AltId UNIQUEIDENTIFIER NOT NULL,
                                        Col1 NVARCHAR(100) NOT NULL
                                        CONSTRAINT AK_Table1_AltId UNIQUE(AltId)
                                    );
                                    """);

        await db.Execute(schema => $"""
                                    CREATE TABLE [{schema}].[Table2](
                                        Id INT IDENTITY(1,1) PRIMARY KEY,
                                        AltId UNIQUEIDENTIFIER NOT NULL,
                                        Table1_Id INT NOT NULL,
                                        Table1_AltId UNIQUEIDENTIFIER NOT NULL,
                                        Col1 NVARCHAR(100) NOT NULL,
                                        FOREIGN KEY (Table1_Id) REFERENCES [{schema}].[Table1](Id),
                                        FOREIGN KEY (Table1_AltId) REFERENCES [{schema}].[Table1](AltId),
                                        CONSTRAINT [AK_Table2_AltId] UNIQUE(AltId)
                                    );
                                    """);
        await db.Execute(schema => $"""
                                    ALTER TABLE [{schema}].[Table1]
                                    ADD FOREIGN KEY (Table2_Id) REFERENCES [{schema}].[Table2](Id)
                                    """);

        await db.Execute(schema => $"""
                                    ALTER TABLE [{schema}].[Table1]
                                    ADD FOREIGN KEY (Table2_AltId) REFERENCES [{schema}].[Table2](AltId);
                                    """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, TableFilters.SchemaEquals(db.SchemaName));
    }


    [Fact]
    public async Task When_there_is_foreign_key_to_alternate_key()
    {
        var db = await Database.CreateDatabase(_testOutputHelper);

        await db.Execute(schema => $"""
                                    CREATE TABLE [{schema}].[Table1](
                                        Id INT IDENTITY(1,1) PRIMARY KEY,
                                        AltId UNIQUEIDENTIFIER NOT NULL,
                                        Col1 NVARCHAR(100) NOT NULL
                                        CONSTRAINT AK_Table1_AltId UNIQUE(AltId)
                                    );
                                    """);

        await db.Execute(schema => $"""
                                    CREATE TABLE [{schema}].[Table2](
                                        Id INT IDENTITY(1,1) PRIMARY KEY,
                                        AltId UNIQUEIDENTIFIER NOT NULL,
                                        Table1_Id INT NOT NULL,
                                        Table1_AltId UNIQUEIDENTIFIER NOT NULL,
                                        Col1 NVARCHAR(100) NOT NULL,
                                        FOREIGN KEY (Table1_Id) REFERENCES [{schema}].[Table1](Id),
                                        FOREIGN KEY (Table1_AltId) REFERENCES [{schema}].[Table1](AltId),
                                        CONSTRAINT [AK_Table2_AltId] UNIQUE(AltId)
                                    );
                                    """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, TableFilters.SchemaEquals(db.SchemaName));
    }

    private class Database
    {
        public string ConnectionString { get; }
        public string SchemaName { get; }

        private Database(string connectionString, string schemaName)
        {
            ConnectionString = connectionString;
            SchemaName = schemaName;
        }

        public static async Task<Database> CreateDatabase(ITestOutputHelper testOutputHelper)
        {
            var dbName = "OwnDataSpaces.Tests";
            var container = new SqlServerContainer(dbName);
            await container.InitializeAsync();
            await using (var masterConnection =
                         new SqlConnection(SqlServerContainer.ReplaceDatabase(container.ConnectionString, "master")))
            {
                await masterConnection.ExecuteAsync($"""
                                                     IF DB_ID (N'{dbName}') IS NULL
                                                          CREATE DATABASE [{dbName}];
                                                     """);
            }

            var schemaName = "OWN_SCHEMA_" + Guid.NewGuid().ToString("N");
            testOutputHelper.WriteLine(schemaName);
            await using (var connection = new SqlConnection(container.ConnectionString))
            {
                await connection.ExecuteAsync($"CREATE SCHEMA [{schemaName}];");
            }

            return new Database(container.ConnectionString, schemaName);
        }

        public async Task Execute(Func<string, string> sql)
        {
            await using var connection = new SqlConnection(ConnectionString);
            await connection.ExecuteAsync(sql(SchemaName));
        }
    }
}