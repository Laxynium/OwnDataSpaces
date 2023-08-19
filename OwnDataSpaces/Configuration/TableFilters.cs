namespace OwnDataSpaces.Configuration;

public delegate bool TableFilter(Table table);

public static class TableFilters
{
    public static TableFilter All(TableFilter[] filters) => t => filters.All(f => f(t));

    public static TableFilter Any(TableFilter[] filters) => t => filters.Any(f => f(t));

    public static TableFilter NameContains(string phrase) =>
        t => t.Name.Contains(phrase, StringComparison.InvariantCultureIgnoreCase);

    public static TableFilter NameStartsWith(string phrase) =>
        t => t.Name.StartsWith(phrase, StringComparison.InvariantCultureIgnoreCase);

    public static TableFilter NameEquals(string name) =>
        t => t.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase);

    public static TableFilter SchemaContains(string phrase) =>
        t => t.Schema.Contains(phrase, StringComparison.InvariantCultureIgnoreCase);

    public static TableFilter SchemaStartsWith(string phrase) =>
        t => t.Schema.StartsWith(phrase, StringComparison.InvariantCultureIgnoreCase);

    public static TableFilter SchemaEquals(string name) =>
        t => t.Schema.Equals(name, StringComparison.InvariantCultureIgnoreCase);

    public static TableFilter And(this TableFilter f1, TableFilter f2) => t => f1(t) && f2(t);

    public static TableFilter Or(this TableFilter f1, TableFilter f2) => t => f1(t) || f2(t);
    public static TableFilter Not(this TableFilter f1) => t => !f1(t);
}