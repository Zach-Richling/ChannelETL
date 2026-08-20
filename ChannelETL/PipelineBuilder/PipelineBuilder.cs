namespace ChannelETL;

internal sealed class PipelineBuilder(IEnumerable<Type> pipelinesInGroup) : IPipelineBuilder
{
    private HashSet<Type> _parentPipelines = [];
    public IEnumerable<Type> ParentPipelines => _parentPipelines;

    public IPipelineBuilder WaitFor<TParentPipeline>() where TParentPipeline : IPipeline
    {
        if (!pipelinesInGroup.Any(x => x == typeof(TParentPipeline)))
        {
            throw new InvalidOperationException($"Pipeline of type {typeof(TParentPipeline).Name} is not in the pipeline group.");
        }

        if (_parentPipelines.Add(typeof(TParentPipeline)))
        {
            return this;
        }

        throw new InvalidOperationException($"Parent pipeline of type {typeof(TParentPipeline).Name} has already been added.");
    }
}
