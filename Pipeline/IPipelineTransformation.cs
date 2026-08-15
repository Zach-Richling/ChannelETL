namespace ChannelETL.Pipeline;

public interface IPipelineTransformation<TSource, TDest>
{
    Task<TDest> TransformAsync(TSource item, CancellationToken token);
}
