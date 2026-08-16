namespace ChannelETL;

public interface IPipelineTransformation<TSource, TDest>
{
    Task<TDest> TransformAsync(TSource item, CancellationToken token);
}
