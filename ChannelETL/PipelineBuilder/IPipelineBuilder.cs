namespace ChannelETL;

public interface IPipelineBuilder
{
    IPipelineBuilder WaitFor<TParentPipeline>() where TParentPipeline : IPipeline;
}
