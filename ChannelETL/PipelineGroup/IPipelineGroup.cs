namespace ChannelETL;

public interface IPipelineGroup
{
    Task RunAsync(PipelineGroupExecutionContext context);
}