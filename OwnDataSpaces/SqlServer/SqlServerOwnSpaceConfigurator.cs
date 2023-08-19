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

            var indexes = await executor.GetUniqueIndexes(table);
            foreach (var index in indexes)
            {
                await executor.AddSpaceIdToIndex(table, index, ownSpaceColumnName);
            }
        }
        
        await executor.DropOwnSpacePolicy(policyName);
        
        await executor.AddOwnSpacePolicyFunction(policyFunction, ownSpaceVariableName);
        
        await executor.AddOwnSpacePolicy(policyName, policyFunction, tables, ownSpaceColumnName);
    }
    
    private record UniqueIndex(string Name, Column[] Columns);
    private record Column(string Name, bool IsDescending, bool IsIncluded);
}