namespace ChannelETL;

public interface IPipelineDestination<TDest>
{
    Task ConsumeAsync(TDest item, CancellationToken token);
}
