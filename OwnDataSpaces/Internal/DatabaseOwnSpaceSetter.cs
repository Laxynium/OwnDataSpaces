﻿using System.Data.Common;
using Dapper;
using Microsoft.Extensions.Logging;

namespace OwnDataSpaces.Internal;

internal class DatabaseOwnSpaceSetter
{
    private readonly OwnSpaceProvider _provider;
    private readonly SetOwnSpaceSqlProvider _sqlProvider;
    private readonly ILogger<DatabaseOwnSpaceSetter> _logger;

    public DatabaseOwnSpaceSetter(
        OwnSpaceProvider provider,
        SetOwnSpaceSqlProvider sqlProvider,
        ILogger<DatabaseOwnSpaceSetter> logger)
    {
        _provider = provider;
        _sqlProvider = sqlProvider;
        _logger = logger;
    }

    public async Task SetOwnSpace(DbConnection connection, CancellationToken ct)
    {
        var spaceId = _provider.GetSpaceId();

        var sql = _sqlProvider(spaceId);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;

        await cmd.ExecuteNonQueryAsync(ct);
    }

    public void SetOwnSpace(DbConnection connection)
    {
        var spaceId = _provider.GetSpaceId();

        var sql = _sqlProvider(spaceId);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;

        cmd.ExecuteNonQuery();
    }
}