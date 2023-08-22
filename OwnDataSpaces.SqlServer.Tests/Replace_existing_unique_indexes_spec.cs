using Xunit.Abstractions;

namespace OwnDataSpaces.SqlServer.Tests;

public class ApplyOwnSpaceOnTablesWithIndexesSpec
{
    private readonly ITestOutputHelper _testOutputHelper;

    public ApplyOwnSpaceOnTablesWithIndexesSpec(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task When_there_is_id_and_some_column()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_id_and_some_column)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_unique_index)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_unique_constraint)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_unique_index_and_unique_constraint)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_unique_constraint_with_many_columns)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_unique_index_with_many_columns)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_foreign_key_to_unique_constraint)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_foreign_key_to_unique_index)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_foreign_key_to_two_unique_indexes)}");

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
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_foreign_key_to_two_unique_constraints)}");

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


    [Fact]
    public async Task Where_there_is_foreign_key_to_primary_key()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(Where_there_is_foreign_key_to_primary_key)}");

        await db.Execute($"""
                          CREATE TABLE [Table1](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Col1 NVARCHAR(100) NOT NULL
                          );
                          """);

        await db.Execute($"""
                          CREATE TABLE [Table2](
                              Id INT IDENTITY(1,1) PRIMARY KEY,
                              Table1Id INT NOT NULL,
                              CONSTRAINT FK_Tablle1Id_Table2_Table1 FOREIGN KEY (Table1Id) REFERENCES [Table1](Id),
                          );
                          """);

        await SqlServerOwnSpaceConfigurator.Apply(db.ConnectionString, _ => true);

        await db.EnsureOwnSpacesAreNotLeaking(
            "INSERT INTO [Table1] (Col1) VALUES('Text123')",
            "SELECT COUNT(1) FROM [Table1]", 1);
    }


    [Fact(Skip = "Looks like there cannot be a foreign key to filtered unique index, " +
                 "keeping this test as a documentation of this fact")]
    public async Task When_there_is_foreign_key_to_filtered_unique_index()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(ApplyOwnSpaceOnTablesWithIndexesSpec)}{nameof(When_there_is_foreign_key_to_filtered_unique_index)}");

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
}