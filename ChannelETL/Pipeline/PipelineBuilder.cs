namespace ChannelETL;

public class PipelineBuilder<TSource, TDest>
{
    private IPipelineSource<TSource>? _source;
    private IPipelineTransformation<TSource, TDest>? _transform;
    private IPipelineDestination<TDest>? _destination;
    private List<IPipeline> _parentPipelines = new();

    public PipelineBuilder<TSource, TDest> WithSource(IPipelineSource<TSource> source)
    {
        _source = source;
        return this;
    }

    public PipelineBuilder<TSource, TDest> WithTransformation(IPipelineTransformation<TSource, TDest> transform)
    {
        _transform = transform;
        return this;
    }

    public PipelineBuilder<TSource, TDest> WithDestination(IPipelineDestination<TDest> destination)
    {
        _destination = destination;
        return this;
    }

    public PipelineBuilder<TSource, TDest> WaitFor(params IPipeline[] parentPipelines)
    {
        _parentPipelines.AddRange(parentPipelines);
        return this;
    }

    public Pipeline<TSource, TDest> Build()
        => Build(Guid.NewGuid().ToString());

    public Pipeline<TSource, TDest> Build(string name)
    {
        ArgumentNullException.ThrowIfNull(_source, nameof(WithSource));
        ArgumentNullException.ThrowIfNull(_transform, nameof(WithTransformation));
        ArgumentNullException.ThrowIfNull(_destination, nameof(WithDestination));

        return new Pipeline<TSource, TDest>()
        {
            Source = _source!,
            Transform = _transform!,
            Destination = _destination!,
            ParentPipelines = new List<IPipeline>(_parentPipelines),
            Name = name
        };
    }
}
