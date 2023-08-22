using System.Runtime.CompilerServices;
using OwnDataSpaces.Configuration;

namespace OwnDataSpaces.SqlServer;

public static partial class SqlServerOwnSpaceConfigurator
{
    public static string SetOwnSpaceSql(Guid spaceId) => FormattableStringFactory
        .Create(SetSessionContext, "OwnSpaceId", spaceId)
        .ToString();

    private static readonly string SetSessionContext =
        "EXEC sp_set_session_context @key=N'{0}', @value='{1:D}', @read_only=1;";

    public static async Task Apply(string connectionString, TableFilter tableFilter)
    {
        await using var executor = await Executor.Create(connectionString);

        var tables = await executor.GetTablesToModify(tableFilter);

        const string ownSpaceColumnName = "OwnSpaceId";
        const string ownSpaceVariableName = "OwnSpaceId";
        const string policyName = "dbo.OwnSpacePolicy";
        const string policyFunction = "dbo.fn_get_own_space_id";

        foreach (var table in tables)
        {
            await executor.AddOwnSpaceColumn(table, ownSpaceColumnName);
            await executor.AddOwnSpaceIdAsDefaultColumnValue(table, ownSpaceVariableName, ownSpaceColumnName);
        }

        var uniqueIndexes = new List<(Table table, List<UniqueIndex>)>();
        foreach (var table in tables)
        {
            var indexes = await executor.GetUniqueIndexes(table);
            uniqueIndexes.Add((table, indexes.ToList()));
        }

        var foreignKeys = new List<ForeignKey>();
        foreach (var (table, indexes) in uniqueIndexes)
        {
            var constraints = indexes.Where(x => x.HasConstraint)
                .SelectMany(x=>x.Columns)
                .Select(x=>x.Name)
                .ToHashSet();
            var uixs = indexes.Where(x => !x.HasConstraint)
                .SelectMany(x=>x.Columns)
                .Select(x=>x.Name)
                .ToHashSet();
            
            var indexForeignKeys = await executor.GetForeignKeys(table);
            var toAdd = indexForeignKeys
                .Where(x => x.ReferenceColumns.Any(c => constraints.Contains(c)))
                .ToList();
            var toAdd2 = indexForeignKeys
                .Where(x => x.ReferenceColumns.Any(c => uixs.Contains(c)))
                .Select(x=> x with {IsConstraint = false})
                .ToList();
            foreignKeys.AddRange(toAdd);
            foreignKeys.AddRange(toAdd2);
        }

        if (foreignKeys.Any(x => !tables.Contains(x.Table)))
        {
            throw new InvalidOperationException("There is a table with foreign key which is not included in filter");
        }

        foreach (var foreignKey in foreignKeys)
        {
            await executor.DropForeignKey(foreignKey);
        }

        foreach (var (table, indexes) in uniqueIndexes)
        {
            foreach (var index in indexes)
            {
                await executor.AddSpaceIdToIndex(table, index, ownSpaceColumnName);
            }
        }

        foreach (var foreignKey in foreignKeys)
        {
            await executor.AddForeignKey(foreignKey);
        }

        await executor.DropOwnSpacePolicy(policyName);

        await executor.AddOwnSpacePolicyFunction(policyFunction, ownSpaceVariableName);

        await executor.AddOwnSpacePolicy(policyName, policyFunction, tables, ownSpaceColumnName);
    }

    private record UniqueIndex(string Name, bool HasConstraint, string? ConstraintName, Column[] Columns);

    private record Column(string Name, bool IsDescending, bool IsIncluded);
}