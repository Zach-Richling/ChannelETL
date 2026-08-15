namespace ChannelETL;

public interface IPipelineDestination<TDest>
{
    /// <summary>
    /// Consumes an item of type TDest and processes it asynchronously.
    /// </summary>
    Task ConsumeAsync(TDest item, CancellationToken token);

    /// <summary>
    /// Signals that the destination has completed processing all items and performs any necessary cleanup or finalization asynchronously.
    /// </summary>
    /// <param name="token"></param>
    /// <returns></returns>
    Task CompleteAsync(CancellationToken token);
}
