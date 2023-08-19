using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using OwnDataSpaces.Internal;

namespace OwnDataSpaces.EFCore;

public class SetOwnSpaceInterceptor : DbConnectionInterceptor
{
    private readonly DatabaseOwnSpaceSetter _databaseOwnSpaceSetter;

    internal SetOwnSpaceInterceptor(DatabaseOwnSpaceSetter databaseOwnSpaceSetter)
    {
        _databaseOwnSpaceSetter = databaseOwnSpaceSetter;
    }

    public override void ConnectionOpened(DbConnection connection, ConnectionEndEventData eventData) =>
        _databaseOwnSpaceSetter.SetOwnSpace(connection);

    public override Task ConnectionOpenedAsync(DbConnection connection, ConnectionEndEventData eventData,
        CancellationToken ct = new()) => _databaseOwnSpaceSetter.SetOwnSpace(connection, ct);
}