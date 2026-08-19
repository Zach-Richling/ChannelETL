namespace ChannelETL.Extensions.DependencyInjection;

public interface IPipelineGroup
{
    Task RunAsync(PipelineGroupExecutionContext context);
}