using System.Data.Common;
using OwnDataSpaces.Internal;

namespace OwnDataSpaces.EFCore;

public class SetOwnSpaceSqlConnection
{
    private readonly DatabaseOwnSpaceSetter _databaseOwnSpaceSetter;

    internal SetOwnSpaceSqlConnection(DatabaseOwnSpaceSetter databaseOwnSpaceSetter)
    {
        _databaseOwnSpaceSetter = databaseOwnSpaceSetter;
    }

    public async Task SetOwnSpace(DbConnection connection, CancellationToken ct)
    {
        await _databaseOwnSpaceSetter.SetOwnSpace(connection, ct);
    }

    public void SetOwnSpace(DbConnection connection)
    {
        _databaseOwnSpaceSetter.SetOwnSpace(connection);
    }
}