﻿using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.EFCore;
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
    public async Task When_there_is_id_and_some_column()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_id_and_some_column)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Col1 NVARCHAR(100) NOT NULL
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (Col1) VALUES('Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_unique_index()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_unique_index)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                          );
                          CREATE UNIQUE INDEX IX_AltId ON [Table1](AltId)
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_unique_constraint()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_unique_constraint)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                              CONSTRAINT AK_Table1_AltId UNIQUE(AltId)
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_unique_index_and_unique_constraint()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_unique_index_and_unique_constraint)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                              CONSTRAINT AK_Table1_AltId UNIQUE(AltId)
                          );
                          CREATE UNIQUE INDEX IX_Col1 ON [Table1](Col1);
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_unique_constraint_with_many_columns()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_unique_constraint_with_many_columns)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                              CONSTRAINT AK_Table1_AltId UNIQUE(AltId, Col1)
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_unique_index_with_many_columns()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_unique_index_with_many_columns)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                          );
                          CREATE UNIQUE INDEX IX_AltId_Col1 ON [Table1](AltId, Col1); 
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_foreign_key_to_unique_constraint()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_foreign_key_to_unique_constraint)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                              CONSTRAINT AK_Table1_AltId UNIQUE(AltId)
                          );
                          """);

        await db.Execute($"""
                          CREATE TABLE [Table2](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Table1AltId UNIQUEIDENTIFIER NOT NULL,
                              CONSTRAINT FK_Tablle1AltId_Table2_Table1 FOREIGN KEY (Table1AltId) REFERENCES [Table1](AltId),
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_foreign_key_to_unique_index()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_foreign_key_to_unique_index)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                          );
                          CREATE UNIQUE INDEX IX_Col1 ON [Table1](Col1)
                          """);

        await db.Execute($"""
                          CREATE TABLE [Table2](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Table1Col1 NVARCHAR(100) NOT NULL,
                              CONSTRAINT FK_Table1Col1_Table2_Table1 FOREIGN KEY (Table1Col1) REFERENCES [Table1](Col1),
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_foreign_key_to_two_unique_indexes()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_foreign_key_to_two_unique_indexes)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                          );
                          CREATE UNIQUE INDEX Ix_AltId_Col1 ON [Table1](AltId, Col1);
                          """);

        await db.Execute($"""
                          CREATE TABLE [Table2](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Table1Alt1 UNIQUEIDENTIFIER NOT NULL,
                              Table1Col1 NVARCHAR(100) NOT NULL,
                              CONSTRAINT FK_Table1Alt1Col1_Table2_Table1 FOREIGN KEY (Table1Alt1, Table1Col1) REFERENCES [Table1](AltId, Col1)
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }

    [Fact]
    public async Task When_there_is_foreign_key_to_two_unique_constraints()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_foreign_key_to_two_unique_constraints)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              Col1 NVARCHAR(100) NOT NULL
                              CONSTRAINT AK_Table1_AltId UNIQUE(AltId, Col1)
                          );
                          """);

        await db.Execute($"""
                          CREATE TABLE [Table2](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Table1Alt1 UNIQUEIDENTIFIER NOT NULL,
                              Table1Col1 NVARCHAR(100) NOT NULL,
                              CONSTRAINT FK_Table1Alt1Col1_Table2_Table1 FOREIGN KEY (Table1Alt1, Table1Col1) REFERENCES [Table1](AltId, Col1)
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, Col1) VALUES(NEWID(), 'Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }


    [Fact(Skip = "Looks like there cannot be a foreign key to filtered unique index, " +
                 "keeping this test as a documentation of this fact")]
    public async Task When_there_is_foreign_key_to_filtered_unique_index()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ReplaceExistingUniqueIndexesSpec)}{nameof(When_there_is_foreign_key_to_filtered_unique_index)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              AltId UNIQUEIDENTIFIER NOT NULL,
                              IsDeleted BIT NOT NULL
                          );
                          CREATE UNIQUE INDEX IX_AltId ON [Table1](AltId)
                          WHERE IsDeleted = 0
                          """);

        await db.Execute($"""
                          CREATE TABLE [Table2](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Table1Alt1 UNIQUEIDENTIFIER NOT NULL,
                              CONSTRAINT FK_Table1Alt1Col1_Table2_Table1 FOREIGN KEY (Table1Alt1) REFERENCES [Table1](AltId)
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (AltId, IsDeleted) VALUES(NEWID(), 0)",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }


    private class Database
    {
        private IServiceProvider _serviceProvider;
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
}