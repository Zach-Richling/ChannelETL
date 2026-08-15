namespace ChannelETL;

public interface IPipeline
{
    /// <summary>
    /// Gets the name of the pipeline.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets a collection of parent pipelines that should complete before this pipeline is ran.
    /// </summary>
    IEnumerable<IPipeline> ParentPipelines { get; }

    /// <summary>
    /// Runs the pipeline asynchronously, passing a cancellation token to allow for graceful cancellation of the operation.
    /// </summary>
    /// <param name="token">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task RunAsync(CancellationToken token);

    /// <summary>
    /// Gets a task that represents the completion of the pipeline's execution.
    /// </summary>
    Task<PipelineOutcome> CompletionTask { get; }
}

public interface IPipeline<TSource, TDestination> : IPipeline
{
    /// <summary>
    /// Gets the source of the pipeline, which provides the data to be processed.
    /// </summary>
    IPipelineSource<TSource> Source { get; }

    /// <summary>
    /// Gets the transformation of the pipeline, which processes the data from the source and produces the destination data.
    /// </summary>
    IPipelineTransformation<TSource, TDestination> Transform { get; }

    /// <summary>
    /// Gets the destination of the pipeline, which consumes the processed data from the transformation.
    /// </summary>
    IPipelineDestination<TDestination> Destination { get; }
}