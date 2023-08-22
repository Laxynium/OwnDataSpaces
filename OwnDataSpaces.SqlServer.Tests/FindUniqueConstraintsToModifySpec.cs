using FluentAssertions;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.SqlServer.Tests.Fixtures;

namespace OwnDataSpaces.SqlServer.Tests;

public class FindUniqueConstraintsToModifySpec
{
    [Fact]
    public async Task primary_keys_are_not_included()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindUniqueConstraintsToModifySpec)}{nameof(primary_keys_are_not_included)}");

        await db.Execute("""
                            CREATE TABLE [Table1](
                                Id INT IDENTITY(1,1) PRIMARY KEY,
                                Col1 UNIQUEIDENTIFIER NOT NULL,
                                CONSTRAINT AK_Table1_Col1 UNIQUE(Col1)
                            )
                         """);

        var result = await FindUniqueConstraintsToModify(db, _ => true);

        result.Should().HaveCount(1);
        var constraint = result.ElementAt(0);
        constraint.Should().BeEquivalentTo(new SqlServerOwnSpaceConfigurator.UniqueConstraint(
            new Table("Table1", "dbo"),
            "AK_Table1_Col1",
            new[] { "Col1" }));
    }

    [Fact]
    public async Task all_columns_are_listed()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindUniqueConstraintsToModifySpec)}{nameof(all_columns_are_listed)}");

        await db.Execute("""
                            CREATE TABLE [Table1](
                                Id INT IDENTITY(1,1) PRIMARY KEY,
                                Col1 UNIQUEIDENTIFIER NOT NULL,
                                Col2 UNIQUEIDENTIFIER NOT NULL,
                                Col3 UNIQUEIDENTIFIER NOT NULL,
                                CONSTRAINT AK_Table1_Col1 UNIQUE(Col1, Col2, Col3)
                            )
                         """);

        var result = await FindUniqueConstraintsToModify(db, _ => true);

        result.Should().HaveCount(1);
        var constraint = result.ElementAt(0);
        constraint.Should().BeEquivalentTo(new SqlServerOwnSpaceConfigurator.UniqueConstraint(
            new Table("Table1", "dbo"),
            "AK_Table1_Col1",
            new[] { "Col1", "Col2", "Col3" }));
    }

    [Fact]
    public async Task unique_indexes_are_not_included()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindUniqueConstraintsToModifySpec)}{nameof(unique_indexes_are_not_included)}");

        await db.Execute("""
                            CREATE TABLE [Table1](
                                Id INT IDENTITY(1,1) PRIMARY KEY,
                                Col1 UNIQUEIDENTIFIER NOT NULL,
                                Col2 UNIQUEIDENTIFIER NOT NULL,
                                Col3 UNIQUEIDENTIFIER NOT NULL,
                                CONSTRAINT AK_Table1_Col1 UNIQUE(Col1, Col2)
                            );
                            CREATE UNIQUE INDEX IX_Col3 ON [Table1](Col3);
                         """);

        var result = await FindUniqueConstraintsToModify(db, _ => true);

        result.Should().HaveCount(1);
        var constraint = result.ElementAt(0);
        constraint.Should().BeEquivalentTo(new SqlServerOwnSpaceConfigurator.UniqueConstraint(
            new Table("Table1", "dbo"),
            "AK_Table1_Col1",
            new[] { "Col1", "Col2" }));
    }

    [Fact]
    public async Task only_constrains_on_table_within_filter()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindUniqueConstraintsToModifySpec)}{nameof(only_constrains_on_table_within_filter)}");

        await db.Execute("""
                            CREATE TABLE [Table1](
                                Id INT IDENTITY(1,1) PRIMARY KEY,
                                Col1 UNIQUEIDENTIFIER NOT NULL,
                                Col2 UNIQUEIDENTIFIER NOT NULL,
                                Col3 UNIQUEIDENTIFIER NOT NULL,
                                CONSTRAINT AK_Table1_Col1 UNIQUE(Col1, Col2)
                            );
                            CREATE UNIQUE INDEX IX_Col3 ON [Table1](Col3);
                         """);

        await db.Execute("CREATE SCHEMA SchemaB;");
        await db.Execute("""
                            CREATE TABLE SchemaB.[Table1](
                                Id INT IDENTITY(1,1) PRIMARY KEY,
                                Col1 UNIQUEIDENTIFIER NOT NULL,
                                Col2 UNIQUEIDENTIFIER NOT NULL,
                                Col3 UNIQUEIDENTIFIER NOT NULL,
                                CONSTRAINT AK_Table1_Col1 UNIQUE(Col1, Col2)
                            );
                            CREATE UNIQUE INDEX IX_Col3 ON SchemaB.[Table1](Col3);
                         """);

        var result = await FindUniqueConstraintsToModify(db, TableFilters.SchemaEquals("SchemaB"));

        result.Should().HaveCount(1);
        var constraint = result.ElementAt(0);
        constraint.Should().BeEquivalentTo(new SqlServerOwnSpaceConfigurator.UniqueConstraint(
            new Table("Table1", "SchemaB"),
            "AK_Table1_Col1",
            new[] { "Col1", "Col2" }));
    }


    private static async Task<IReadOnlyCollection<SqlServerOwnSpaceConfigurator.UniqueConstraint>>
        FindUniqueConstraintsToModify(
            Database db, TableFilter filter)
    {
        var executor = await SqlServerOwnSpaceConfigurator.Executor.Create(db.ConnectionString);
        return await executor.GetUniqueConstraintsToModify(filter);
    }
}