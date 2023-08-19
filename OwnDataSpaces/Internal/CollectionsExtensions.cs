namespace OwnDataSpaces.Internal;

public static class CollectionsExtensions
{
    public static string Format<T>(this IEnumerable<T> items,
        string delimiter,
        Func<T, FormattableString> template)
    {
        var format = string.Join(delimiter, items.Select(template).Select(x => x.ToString()));
        return format;
    }

    public static string FillWith<T>(this string template, IEnumerable<T> items, Func<T, object[]> parameters,
        string delimiter = ",") =>
        items.Select(x =>
        {
            var objects = parameters(x);
            var format = string.Format(template, objects);
            return format;
        }).JoinUsing(delimiter);

    public static string JoinUsing<T>(this IEnumerable<T> items, string delimiter = ",") =>
        string.Join(delimiter, items);
}