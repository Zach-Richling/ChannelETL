namespace ChannelETL.Extensions.DependencyInjection;

public interface IPipelineBuilder
{
    IPipelineBuilder WaitFor<TParentPipeline>() where TParentPipeline : IPipeline;
}
