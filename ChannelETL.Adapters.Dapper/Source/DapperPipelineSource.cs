using Dapper;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace ChannelETL.Adapters.Dapper;

public abstract class DapperPipelineSource<TSource>(DbConnection connection) : IPipelineSource<TSource>
{
    protected CommandType CommandType { get; init; } = CommandType.Text;
    protected string? Text { get; init; }
    protected string? StoredProcedureName { get; init; }
    protected object? Parameters { get; init; }

    protected string Sql => CommandType switch
    {
        CommandType.Text => Text ?? throw new ArgumentException("Text must be provided for CommandType.Text.", nameof(Text)),
        CommandType.StoredProcedure => StoredProcedureName ?? throw new ArgumentException("StoredProcedureName must be provided for CommandType.StoredProcedure.", nameof(StoredProcedureName)),
        _ => throw new NotSupportedException($"CommandType '{CommandType}' is not supported.")
    };

    public async IAsyncEnumerable<TSource> ProduceAsync([EnumeratorCancellation] CancellationToken token)
    {
        await foreach (var item in connection.QueryUnbufferedAsync<TSource>(Sql, Parameters, commandType: CommandType).WithCancellation(token))
        {
            yield return item;
        }
    }
}
