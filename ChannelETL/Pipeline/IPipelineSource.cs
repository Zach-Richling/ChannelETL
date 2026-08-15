namespace ChannelETL;

public interface IPipelineSource<TSource>
{
    IAsyncEnumerable<TSource> ProduceAsync(CancellationToken token);
}
