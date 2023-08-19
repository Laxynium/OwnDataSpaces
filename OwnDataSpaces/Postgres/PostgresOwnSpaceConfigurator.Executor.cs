using System.Data.Common;
using System.Text.RegularExpressions;
using Dapper;
using Npgsql;
using OwnDataSpaces.Configuration;

namespace OwnDataSpaces.Postgres;

public static partial class PostgresOwnSpaceConfigurator
{
    private class Executor : IAsyncDisposable
    {
        private readonly DbConnection _connection;

        private Executor(DbConnection connection)
        {
            _connection = connection;
        }

        public static async Task<Executor> Create(string connectionString)
        {
            var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            return new Executor(connection);
        }

        public Task CreateAppUser(string username, string password) => ExecuteAsync($"""
                DROP ROLE IF EXISTS "{username}";
                CREATE ROLE "{username}" WITH LOGIN NOBYPASSRLS PASSWORD '{password}';
                GRANT pg_write_all_data, pg_read_all_data TO "{username}";
            """);

        public async Task<IReadOnlyCollection<Table>> GetTablesToModify(TableFilter filter)
        {
            const string sql = """            
                SELECT  "table_name" AS "Name"
                     ,   "table_schema" AS "Schema"
                FROM information_schema.tables
                WHERE "table_schema" != 'pg_catalog' AND
                      "table_schema" != 'information_schema'
            """;
            var tables = await QueryAsync<Table>(sql);

            return tables.Where(x => filter(x)).ToList();
        }

        public async Task AddOwnSpaceColumn(Table table, string columnName) => await ExecuteAsync($"""            
                ALTER TABLE "{table.Schema}"."{table.Name}"
                ADD COLUMN  IF NOT EXISTS "{columnName}" uuid
            """);

        public async Task AddOwnSpaceIdAsDefaultColumnValue(Table table, string columnName) =>
            await ExecuteAsync($"""            
                ALTER TABLE "{table.Schema}"."{table.Name}"
                ALTER COLUMN "{columnName}" SET DEFAULT current_setting('{OwnSpaceVariableName}')::uuid
            """);

        public async Task<IReadOnlyCollection<UniqueIndex>> GetUniqueIndexes(Table table)
        {
            string sql = $"""
                select idx.relname as IndexName,                   
                        pis.indexdef as IndexDef
                from pg_index pgi
                         join pg_class idx on idx.oid = pgi.indexrelid
                         join pg_namespace insp on insp.oid = idx.relnamespace
                         join pg_class tbl on tbl.oid = pgi.indrelid
                         join pg_namespace tnsp on tnsp.oid = tbl.relnamespace
                         join pg_indexes pis on pis.schemaname = tnsp.nspname AND
                                    pis.tablename = tbl.relname AND
                                    pis.indexname = idx.relname
                where pgi.indisunique and
                      not pgi.indisprimary and
                    tnsp.nspname != 'pg_catalog' AND 
                    tnsp.nspname != 'information_schema' AND
                    tnsp.nspname != 'pg_toast' AND
                    tnsp.nspname = '{table.Schema}' AND
                    tbl.relname = '{table.Name}'
            """;
            var result = await _connection.QueryAsync<UniqueIndex>(sql);
            return result.ToList();
        }

        public async Task AddSpaceIdToIndex(Table table, UniqueIndex index, string columnName)
        {
            var updatedIndexDefinition = Regex.Replace(index.IndexDef,
                @"(.*)ON(.+)\((.+)\)(.*)",
                $"$1ON$2($3, \"{columnName}\")$4");

            var sql = $"""
                DROP INDEX IF EXISTS "{table.Schema}"."{index.IndexName}";
                {updatedIndexDefinition};
            """;
            await _connection.ExecuteAsync(sql);
        }

        public async Task EnableRls(Table table) => await _connection.ExecuteAsync($"""
                ALTER TABLE "{table.Schema}"."{table.Name}" FORCE ROW LEVEL SECURITY;
                ALTER TABLE "{table.Schema}"."{table.Name}" ENABLE ROW LEVEL SECURITY;
            """);

        public async Task CreateOwnSpacePolicy(Table table, string username, string columnName,
            string contextVariableName) => await _connection.ExecuteAsync($"""
                DROP POLICY IF EXISTS ownspacepolicy on "{table.Schema}"."{table.Name}";
                CREATE POLICY ownspacepolicy ON "{table.Schema}"."{table.Name}"                
                FOR ALL
                TO "{username}"
                USING ("{columnName}" = current_setting('{contextVariableName}')::uuid) 
            """);

        private Task ExecuteAsync(string sql) => _connection.ExecuteAsync(sql);

        private Task<IEnumerable<T>> QueryAsync<T>(string sql) => _connection.QueryAsync<T>(sql);

        public ValueTask DisposeAsync() => _connection.DisposeAsync();
    }
}