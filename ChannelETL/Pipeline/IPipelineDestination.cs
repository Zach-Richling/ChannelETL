namespace ChannelETL.Pipeline;

public interface IPipelineDestination<TDest>
{
    Task ConsumeAsync(TDest item, CancellationToken token);
}
