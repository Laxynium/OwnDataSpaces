using FluentAssertions;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.SqlServer.Tests.Fixtures;

namespace OwnDataSpaces.SqlServer.Tests;

[Collection(nameof(IntegrationTestFixtureCollection))]
public class FindForeignKeysToModifySpec
{
    [Fact]
    public async Task there_are_only_foreign_keys_from_user_defined_tables()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindForeignKeysToModifySpec)}{nameof(there_are_only_foreign_keys_from_user_defined_tables)}");

        await db.Execute("CREATE TABLE TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                             FOREIGN KEY (TableAId) REFERENCES TableA(Id)
                         );
                         """);

        await db.Execute("CREATE SCHEMA SchemaA;");
        await db.Execute("CREATE TABLE SchemaA.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE SchemaA.TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                             FOREIGN KEY (TableAId) REFERENCES SchemaA.TableA(Id)
                         );
                         """);


        await db.Execute("CREATE SCHEMA SchemaB;");
        await db.Execute("CREATE TABLE SchemaB.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");

        var result = await FindForeignKeysToModify(db, _ => true);

        result.Should().HaveCount(2);
        result
            .Should().SatisfyRespectively(
                x =>
                {
                    x.Table.Should().Be(new Table("TableA", "dbo"));
                    x.ReferencingTable.Should().Be(new Table("TableB", "dbo"));
                    x.ReferencingColumns.Should().BeEquivalentTo(new[] { "Id" });
                    x.Columns.Should().BeEquivalentTo(new[] { "TableAId" });
                },
                x =>
                {
                    x.Table.Should().Be(new Table("TableA", "SchemaA"));
                    x.ReferencingTable.Should().Be(new Table("TableB", "SchemaA"));
                    x.ReferencingColumns.Should().BeEquivalentTo(new[] { "Id" });
                    x.Columns.Should().BeEquivalentTo(new[] { "TableAId" });
                });
    }

    [Fact]
    public async Task when_there_are_cross_schema_foreign_keys()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindForeignKeysToModifySpec)}{nameof(when_there_are_cross_schema_foreign_keys)}");

        await db.Execute("CREATE TABLE TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        ;

        await db.Execute("CREATE SCHEMA SchemaA;");
        await db.Execute("CREATE TABLE SchemaA.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE SchemaA.TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                             FOREIGN KEY (TableAId) REFERENCES dbo.TableA(Id)
                         );
                         """);

        var result = await FindForeignKeysToModify(db, _ => true);

        result.Should().HaveCount(1);
        result
            .Should().SatisfyRespectively(
                x =>
                {
                    x.Table.Should().Be(new Table("TableA", "dbo"));
                    x.ReferencingTable.Should().Be(new Table("TableB", "SchemaA"));
                    x.ReferencingColumns.Should().BeEquivalentTo(new[] { "Id" });
                    x.Columns.Should().BeEquivalentTo(new[] { "TableAId" });
                });
    }


    [Fact]
    public async Task foreign_key_referencing_a_table_outside_of_a_filter_is_not_included()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindForeignKeysToModifySpec)}{nameof(foreign_key_referencing_a_table_outside_of_a_filter_is_not_included)}");

        await db.Execute("CREATE SCHEMA SchemaA;");
        await db.Execute("CREATE TABLE SchemaA.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE SchemaA.TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                             FOREIGN KEY (TableAId) REFERENCES SchemaA.TableA(Id)
                         );
                         """);

        await db.Execute("CREATE SCHEMA SchemaB;");
        await db.Execute("CREATE TABLE SchemaB.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE SchemaB.TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                             FOREIGN KEY (TableAId) REFERENCES SchemaB.TableA(Id)
                         );
                         """);

        var result = await FindForeignKeysToModify(db, TableFilters.SchemaEquals("SchemaA"));

        result.Should().HaveCount(1);
        result
            .Should().SatisfyRespectively(
                x =>
                {
                    x.Table.Should().Be(new Table("TableA", "SchemaA"));
                    x.ReferencingTable.Should().Be(new Table("TableB", "SchemaA"));
                    x.ReferencingColumns.Should().BeEquivalentTo(new[] { "Id" });
                    x.Columns.Should().BeEquivalentTo(new[] { "TableAId" });
                });
    }

    [Fact]
    public async Task foreign_key_on_table_outside_of_filter_but_referencing_a_table_inside_filter_is_included()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindForeignKeysToModifySpec)}{nameof(foreign_key_on_table_outside_of_filter_but_referencing_a_table_inside_filter_is_included)}");

        await db.Execute("CREATE SCHEMA SchemaA;");
        await db.Execute("CREATE TABLE SchemaA.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE SchemaA.TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                         );
                         """);

        await db.Execute("CREATE SCHEMA SchemaB;");
        await db.Execute("CREATE TABLE SchemaB.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("""
                         CREATE TABLE SchemaB.TableB(
                             Id INT IDENTITY(1,1) PRIMARY KEY,
                             TableAId INT NOT NULL
                             FOREIGN KEY (TableAId) REFERENCES SchemaA.TableA(Id)
                         );
                         """);

        var result = await FindForeignKeysToModify(db, TableFilters.SchemaEquals("SchemaA"));

        result.Should().HaveCount(1);
        result
            .Should().SatisfyRespectively(
                x =>
                {
                    x.Table.Should().Be(new Table("TableA", "SchemaA"));
                    x.ReferencingTable.Should().Be(new Table("TableB", "SchemaB"));
                    x.ReferencingColumns.Should().BeEquivalentTo(new[] { "Id" });
                    x.Columns.Should().BeEquivalentTo(new[] { "TableAId" });
                });
    }


    private static async Task<IReadOnlyCollection<SqlServerOwnSpaceConfigurator.ForeignKey>> FindForeignKeysToModify(
        Database db, TableFilter filter)
    {
        var executor = await SqlServerOwnSpaceConfigurator.Executor.Create(db.ConnectionString);
        return await executor.GetForeignKeysToModify(filter);
    }
}