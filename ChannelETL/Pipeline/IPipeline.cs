namespace ChannelETL;

public interface IPipeline
{
    /// <summary>
    /// Runs the pipeline asynchronously, passing a cancellation token to allow for graceful cancellation of the operation.
    /// </summary>
    /// <param name="token">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task RunAsync(PipelineExecutionContext context);

    /// <summary>
    /// Gets a task that represents the completion of the pipeline's execution.
    /// </summary>
    Task<PipelineOutcome> CompletionTask { get; }
}