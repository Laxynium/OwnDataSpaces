namespace OwnDataSpaces.Configuration;

public class OwnSpacesOptions
{
    internal string Type { get; set; } = string.Empty;

    public OwnSpacesOptions UseSqlServer()
    {
        Type = "SqlServer";
        return this;
    }
    public OwnSpacesOptions UsePostgres()
    {
        Type = "Postgres";
        return this;
    }
}