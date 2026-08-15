namespace ChannelETL.Pipeline;

public interface IPipelineSource<TSource>
{
    IAsyncEnumerable<TSource> ProduceAsync(CancellationToken token);
}
