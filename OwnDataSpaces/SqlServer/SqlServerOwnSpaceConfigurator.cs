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

        var foreignKeysToModify = await executor.GetForeignKeysToModify(tableFilter);
        var uniqueConstraints = await executor.GetUniqueConstraintsToModify(tableFilter);
        var uniqueIndexesToModify = await executor.GetUniqueIndexesToModify(tableFilter);

        var invalidForeignKeys = foreignKeysToModify
            .Where(x => tables.All(t => t != x.ReferencingTable))
            .ToList();
        if (invalidForeignKeys.Any())
        {
            var invalidForeignKey = invalidForeignKeys[0];
            throw new InvalidOperationException(
                $"There is foreign key {invalidForeignKey.Name} from table {invalidForeignKey.ReferencingTable} " +
                $"to table {invalidForeignKey.Table} where referencing table is outside of provided filter");
        }

        var foreignKeysReferencingConstraints = foreignKeysToModify
            .Where(fk => uniqueConstraints.Any(x => x.Table == fk.Table && fk.Columns.SequenceEqual(x.Columns)))
            .Select(fk => new
            {
                fk,
                constraint = uniqueConstraints.First(x => x.Table == fk.Table && fk.Columns.SequenceEqual(x.Columns))
            })
            .ToList();

        var foreignKeysReferencingIndexes = foreignKeysToModify
            .Where(fk =>
                uniqueIndexesToModify.Any(x =>
                    x.Table == fk.Table && fk.Columns.SequenceEqual(x.Columns.Select(y => y.Name))))
            .Select(fk => new
            {
                fk,
                constraint = uniqueIndexesToModify.First(x =>
                    x.Table == fk.Table && fk.Columns.SequenceEqual(x.Columns.Select(y => y.Name)))
            })
            .ToList();


        var foreignKeysToRecreate = foreignKeysReferencingConstraints.Select(x => x.fk)
            .Concat(foreignKeysReferencingIndexes.Select(x => x.fk))
            .DistinctBy(x => new { x.ReferencingTable, x.Name })
            .ToList();

        foreach (var fk in foreignKeysToRecreate)
        {
            await executor.DropForeignKey(fk);
        }

        foreach (var uniqueConstraint in uniqueConstraints)
        {
            await executor.ReplaceUniqueConstraint(uniqueConstraint, ownSpaceColumnName);
        }

        foreach (var uniqueIndex in uniqueIndexesToModify)
        {
            await executor.ReplaceUniqueIndex(uniqueIndex, ownSpaceColumnName);
        }

        foreach (var fk in foreignKeysToRecreate)
        {
            await executor.RecreateForeignKey(fk, ownSpaceColumnName);
        }

        await executor.DropOwnSpacePolicy(policyName);

        await executor.AddOwnSpacePolicyFunction(policyFunction, ownSpaceVariableName);

        await executor.AddOwnSpacePolicy(policyName, policyFunction, tables, ownSpaceColumnName);
    }
}