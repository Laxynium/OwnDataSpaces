using OwnDataSpaces.Configuration;

namespace OwnDataSpaces.Postgres;

public static partial class PostgresOwnSpaceConfigurator
{
    private const string OwnSpaceVariableName = "app.own_space_id";
    private const string OwnSpaceColumnName = "own_space_id";

    public static string SetOwnSpaceSql(Guid spaceId) =>
        string.Format(SetSessionContext, OwnSpaceVariableName, spaceId);

    private const string SetSessionContext = """SET {0} = '{1:D}'""";

    public static async Task Apply(string connectionString, TableFilter tableFilter, string user, string password)
    {
        await using var executor = await Executor.Create(connectionString);

        await executor.CreateAppUser(user, password);

        var tables = await executor.GetTablesToModify(tableFilter);

        foreach (var table in tables)
        {
            await executor.AddOwnSpaceColumn(table, OwnSpaceColumnName);
            await executor.AddOwnSpaceIdAsDefaultColumnValue(table, OwnSpaceColumnName);

            // maybe it would be better to have single query
            var indexes = await executor.GetUniqueIndexes(table);
            foreach (var index in indexes)
            {
                await executor.AddSpaceIdToIndex(table, index, OwnSpaceColumnName);
            }

            await executor.EnableRls(table);

            await executor.CreateOwnSpacePolicy(
                table,
                user,
                OwnSpaceColumnName,
                OwnSpaceVariableName);
        }
    }

    private record UniqueIndex(string IndexName, string IndexDef);
}