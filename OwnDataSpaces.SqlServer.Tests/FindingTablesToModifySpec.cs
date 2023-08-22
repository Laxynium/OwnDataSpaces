using FluentAssertions;
using OwnDataSpaces.Configuration;
using OwnDataSpaces.SqlServer.Tests.Fixtures;

namespace OwnDataSpaces.SqlServer.Tests;

[Collection(nameof(DatabaseTestsCollection))]
public class FindingTablesToModifySpec
{
    [Fact]
    public async Task there_are_only_user_defined_tables_when_no_filter_provided()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindingTablesToModifySpec)}{nameof(there_are_only_user_defined_tables_when_no_filter_provided)}");

        await db.Execute("CREATE TABLE TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("CREATE TABLE TableB(Id INT IDENTITY(1,1) PRIMARY KEY)");
        
        await db.Execute("CREATE SCHEMA SchemaA;");
        await db.Execute("CREATE TABLE SchemaA.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("CREATE TABLE SchemaA.TableB(Id INT IDENTITY(1,1) PRIMARY KEY)");

        await db.Execute("CREATE SCHEMA SchemaB;");
        await db.Execute("CREATE TABLE SchemaB.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        
        var result = await FindTablesToModify(db, _ => true);

        result.Should().HaveCount(5);
        result.Should().NotContain(x => x.Schema != "dbo" && x.Schema != "SchemaA" && x.Schema != "SchemaB");
    }
    
    [Fact]
    public async Task user_defined_tables_are_narrowed_when_filter_is_provided()
    {
        var db = await Database.CreateDatabase(
            $"{nameof(FindingTablesToModifySpec)}{nameof(user_defined_tables_are_narrowed_when_filter_is_provided)}");

        await db.Execute("CREATE TABLE TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("CREATE TABLE TableB(Id INT IDENTITY(1,1) PRIMARY KEY)");
        
        await db.Execute("CREATE SCHEMA SchemaA;");
        await db.Execute("CREATE TABLE SchemaA.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        await db.Execute("CREATE TABLE SchemaA.TableB(Id INT IDENTITY(1,1) PRIMARY KEY)");

        await db.Execute("CREATE SCHEMA SchemaB;");
        await db.Execute("CREATE TABLE SchemaB.TableA(Id INT IDENTITY(1,1) PRIMARY KEY)");
        
        var result = await FindTablesToModify(db, TableFilters.SchemaEquals("SchemaA"));

        result.Should().HaveCount(2);
        result.Should().NotContain(x => x.Schema != "SchemaA");
    }

    private static async Task<IReadOnlyCollection<Table>> FindTablesToModify(Database db, TableFilter filter)
    {
        var executor = await SqlServerOwnSpaceConfigurator.Executor.Create(db.ConnectionString);
        return await executor.GetTablesToModify(filter);
    }
}