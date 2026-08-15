namespace ChannelETL.Pipeline;

public interface IPipeline
{
    Task RunAsync(CancellationToken token);
}
public interface IPipeline<TSource, TDestination> : IPipeline
{
    IPipelineDestination<TDestination> Destination { get; init; }
    IPipelineSource<TSource> Source { get; init; }
    IPipelineTransformation<TSource, TDestination> Transform { get; init; }
}