using System.Collections.Concurrent;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

internal static class DataTableExtensions
{
    private static readonly ConcurrentDictionary<Type, object> _delegateCache = new();

    public static DataTable ToDataTable<T>(this IEnumerable<T> data)
    {
        var table = new DataTable(typeof(T).Name);
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var prop in properties)
        {
            var propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
            table.Columns.Add(prop.Name, propType);
        }

        var extractor = (Func<T, object[]>)_delegateCache.GetOrAdd(typeof(T), _ => CompileExtractor<T>(properties));

        table.BeginLoadData();
        try
        {
            foreach (T item in data)
            {
                if (item != null)
                {
                    var extracted = extractor(item);
                    table.Rows.Add(extracted);
                }
            }
        }
        finally
        {
            table.EndLoadData();
        }

        return table;
    }

    private static Func<T, object[]> CompileExtractor<T>(PropertyInfo[] properties)
    {
        var inputParam = Expression.Parameter(typeof(T), "item");
        var elementExpressions = new Expression[properties.Length];

        for (int i = 0; i < properties.Length; i++)
        {
            var propAccess = Expression.Property(inputParam, properties[i]);
            var convert = Expression.Convert(propAccess, typeof(object));

            elementExpressions[i] = Expression.Coalesce(
                convert,
                Expression.Constant(DBNull.Value, typeof(object))
            );
        }

        var arrayExpression = Expression.NewArrayInit(typeof(object), elementExpressions);

        return Expression.Lambda<Func<T, object[]>>(arrayExpression, inputParam).Compile();
    }
}