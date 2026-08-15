namespace ChannelETL;

public interface IPipeline
{
    //A name to identify the pipeline
    string Name { get; }

    //A collection of parent pipelines that should complete before this pipeline is ran
    IEnumerable<IPipeline> ParentPipelines { get; }

    //Run the pipeline asynchronously, passing a cancellation token to allow for graceful cancellation of the operation
    Task RunAsync(CancellationToken token);

    //A task that represents the completion of the pipeline's execution
    Task<PipelineOutcome> CompletionTask { get; }
}

public interface IPipeline<TSource, TDestination> : IPipeline
{
    //The source of the pipeline, which provides the data to be processed
    IPipelineSource<TSource> Source { get; }

    //The transformation of the pipeline, which processes the data from the source and produces the destination data
    IPipelineTransformation<TSource, TDestination> Transform { get; }

    //The destination of the pipeline, which consumes the processed data from the transformation
    IPipelineDestination<TDestination> Destination { get; }
}