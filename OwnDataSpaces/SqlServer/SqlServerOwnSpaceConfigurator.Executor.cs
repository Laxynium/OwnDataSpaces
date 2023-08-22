using System.Data.Common;
using Dapper;
using OwnDataSpaces.Internal;
using Microsoft.Data.SqlClient;
using OwnDataSpaces.Configuration;

namespace OwnDataSpaces.SqlServer;

public static partial class SqlServerOwnSpaceConfigurator
{
    private class Executor
    {
        private readonly DbConnection _connection;

        private Executor(DbConnection connection)
        {
            _connection = connection;
        }

        public static async Task<Executor> Create(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return new Executor(connection);
        }

        public async Task<IReadOnlyCollection<Table>> GetTablesToModify(TableFilter filter)
        {
            const string sql = """
                                   SELECT
                                         TABLE_NAME as [Name]
                                       , TABLE_SCHEMA as [Schema]
                                   FROM INFORMATION_SCHEMA.TABLES
                                   WHERE TABLE_TYPE = 'BASE TABLE';
                               """;
            var tables = await QueryAsync<Table>(sql);
            return tables.Where(x => filter(x)).ToList();
        }

        public async Task AddOwnSpaceColumn(Table table, string columnName)
        {
            var sql = $"""            
                           IF NOT EXISTS(SELECT 1
                               FROM   sys.columns
                               WHERE  object_id = OBJECT_ID(N'[{table.Schema}].[{table.Name}]')
                                 AND name = '{columnName}')
                           BEGIN
                               ALTER TABLE [{table.Schema}].[{table.Name}]
                               ADD [{columnName}] uniqueidentifier;
                           END
                       """;
            await ExecuteAsync(sql);
        }

        public async Task AddOwnSpaceIdAsDefaultColumnValue(Table table, string contextVariableName, string columnName)
        {
            var sql = $"""
                           ALTER TABLE [{table.Schema}].[{table.Name}] DROP CONSTRAINT IF EXISTS df_{columnName}_{table.Name};
                           
                           ALTER TABLE [{table.Schema}].[{table.Name}]
                           ADD CONSTRAINT df_{columnName}_{table.Name}
                           DEFAULT CAST(SESSION_CONTEXT(N'{contextVariableName}') AS uniqueidentifier) FOR [{columnName}];           
                       """;
            await ExecuteAsync(sql);
        }

        public async Task<IReadOnlyCollection<UniqueIndex>> GetUniqueIndexes(Table table)
        {
            var sql = $"""            
                           SELECT
                                 i.name AS IndexName
                               , CAST (IIF((i.type_desc <> 'NONCLUSTERED'), 1, 0) AS BIT) AS IsClustered
                               , COL_NAME(c.object_id, c.column_id) AS ColumnName
                               , c.is_descending_key AS IsDescending
                               , c.is_included_column AS IsIncluded
                               , IIF(kc.type = 'UQ', CONVERT(BIT, 1), CONVERT(BIT, 0)) AS HasConstraint
                               , kc.name AS ConstraintName
                           FROM sys.indexes i
                               INNER JOIN sys.index_columns AS c
                                   ON i.object_id = c.object_id AND i.index_id = c.index_id
                               LEFT JOIN sys.key_constraints kc ON i.index_id = kc.unique_index_id AND i.object_id = kc.parent_object_id
                           WHERE i.is_hypothetical = 0 AND
                                 i.is_unique = 1 AND
                                 i.is_primary_key = 0 AND
                                 i.object_id = OBJECT_ID('[{table.Schema}].[{table.Name}]')
                       """;

            var columns = await QueryAsync(sql, () => new
            {
                IndexName = default(string)!,
                IsClustered = default(bool),
                ColumnName = default(string)!,
                IsDescending = default(bool),
                IsIncluded = default(bool),
                HasConstraint = default(bool),
                ConstraintName = default(string)
            });

            return columns.GroupBy(x => new
                    { x.IndexName, ConstriantName = x.HasConstraint ? x.ConstraintName : string.Empty })
                .Select(g => new UniqueIndex(
                    g.Key.IndexName,
                    g.Key.ConstriantName != string.Empty,
                    g.Key.ConstriantName, g
                        .Select(x => new Column(x.ColumnName, x.IsDescending, x.IsIncluded))
                        .ToArray()))
                .ToList();
        }

        public async Task AddSpaceIdToIndex(Table table, UniqueIndex index, string columnName)
        {
            if (index.Columns.Any(c => c.Name == columnName))
            {
                return;
            }

            var columns = index.Columns.Where(x => !x.IsIncluded)
                .Append(new Column(columnName, true, false));

            if (index.HasConstraint)
            {
                var sql = $"""
                            ALTER TABLE [{table.Schema}].[{table.Name}]
                            DROP CONSTRAINT [{index.ConstraintName}];
                            ALTER TABLE [{table.Schema}].[{table.Name}]
                            ADD CONSTRAINT [{index.ConstraintName}] UNIQUE
                                ({columns.SkipLast(1).Format(",", c => $"""[{c.Name}]""")})
                           """;
                await ExecuteAsync(sql);
            }
            else
            {
                var sql = $"""            
                               DROP INDEX [{table.Schema}].[{table.Name}].[{index.Name}];
                               CREATE UNIQUE NONCLUSTERED INDEX [{index.Name}]
                               ON [{table.Schema}].[{table.Name}]
                                   ({columns.Format(",", c => $"""
                                                                   [{c.Name}] {(c.IsDescending ? "DESC" : "ASC")}
                                                               """)})
                           """;
                await ExecuteAsync(sql);
            }
        }

        public async Task DropOwnSpacePolicy(string policyName) =>
            await ExecuteAsync($@"DROP SECURITY POLICY IF EXISTS {policyName}");

        public async Task AddOwnSpacePolicyFunction(string functionName, string contextVariableName)
        {
            var sql = $"""
                           CREATE OR ALTER FUNCTION {functionName}(@SpaceId uniqueidentifier)
                           RETURNS TABLE
                           WITH SCHEMABINDING
                           AS
                           RETURN SELECT 1 AS Result
                                  WHERE CAST(SESSION_CONTEXT(N'{contextVariableName}') AS uniqueidentifier) = @SpaceId
                       """;
            await ExecuteAsync(sql);
        }

        public async Task AddOwnSpacePolicy(
            string policyName,
            string policyFunction,
            IEnumerable<Table> tables,
            string columnName)
        {
            var sql = $"""                            
                           CREATE SECURITY POLICY {policyName}
                           {tables.Format(",", t => $"""                
                                                       ADD FILTER PREDICATE {policyFunction}([{columnName}]) ON [{t.Schema}].[{t.Name}]
                                                     , ADD BLOCK PREDICATE {policyFunction}([{columnName}]) ON [{t.Schema}].[{t.Name}] AFTER INSERT
                                                     """
                           )}
                           WITH (STATE = ON, SCHEMABINDING = ON);
                       """;
            await ExecuteAsync(sql);
        }

        public async Task<List<ForeignKey>> GetForeignKeys(Table table)
        {
            var sql = $"""
                       EXEC sp_fkeys @pktable_name='{table.Name}', @pktable_owner='{table.Schema}'
                       """;

            var sqlResult = await QueryAsync(sql, () => new
            {
                PKTABLE_QUALIFIER = default(string)!,
                PKTABLE_OWNER = default(string)!,
                PKTABLE_NAME = default(string)!,
                PKCOLUMN_NAME = default(string)!,
                FKTABLE_QUALIFIER = default(string)!,
                FKTABLE_OWNER = default(string)!,
                FKTABLE_NAME = default(string)!,
                FKCOLUMN_NAME = default(string)!,
                KEY_SEQ = default(short),
                UPDATE_RULE = default(short),
                DELETE_RULE = default(short),
                FK_NAME = default(string)!,
                PK_NAME = default(string)!,
                DEFERRABILITY = default(short),
            });

            var result = sqlResult.GroupBy(x => new { x.FKTABLE_OWNER, x.FKTABLE_NAME })
                .SelectMany(x => x
                    .GroupBy(y => y.FK_NAME)
                    .Select(y => new ForeignKey(
                        new Table(x.Key.FKTABLE_NAME, x.Key.FKTABLE_OWNER), table, y.Key,
                        y.Select(z => z.FKCOLUMN_NAME).ToArray(),
                        y.Select(z => z.PKCOLUMN_NAME).ToArray()))
                    .ToList())
                .ToList();

            return result;
        }

        public async Task DropForeignKey(ForeignKey foreignKey)
        {
            var sql = $"""
                       ALTER TABLE [{foreignKey.Table.Schema}].[{foreignKey.Table.Name}]
                       DROP CONSTRAINT [{foreignKey.Name}];
                       """;
            await ExecuteAsync(sql);
        }

        public async Task AddForeignKey(ForeignKey foreignKey)
        {
            var sql = $"""
                       ALTER TABLE [{foreignKey.Table.Schema}].[{foreignKey.Table.Name}]
                       ADD CONSTRAINT [{foreignKey.Name}]
                           FOREIGN KEY ({foreignKey.Columns.Format(",", c => $"[{c}]")})
                           REFERENCES [{foreignKey.ReferenceTable.Schema}].[{foreignKey.ReferenceTable.Name}]({foreignKey.ReferenceColumns.Format(",", c => $"[{c}]")})
                       """;
            await ExecuteAsync(sql);
        }

        private Task ExecuteAsync(string sql) => _connection.ExecuteAsync(sql);

        private Task<IEnumerable<T>> QueryAsync<T>(string sql) => _connection.QueryAsync<T>(sql);

        private Task<IEnumerable<T>> QueryAsync<T>(string sql, Func<T> typeBuilder) => _connection.QueryAsync<T>(sql);

        public ValueTask DisposeAsync() => _connection.DisposeAsync();
    }

    public record ForeignKey(Table Table, Table ReferenceTable, string Name, string[] Columns,
        string[] ReferenceColumns);
}