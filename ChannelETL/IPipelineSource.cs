namespace ChannelETL;

public interface IPipelineSource<TSource>
{
    //TODO: Add a BeginAsync method to let the user take action before the pipeline starts producing data.
    IAsyncEnumerable<TSource> ProduceAsync(CancellationToken token);
}
