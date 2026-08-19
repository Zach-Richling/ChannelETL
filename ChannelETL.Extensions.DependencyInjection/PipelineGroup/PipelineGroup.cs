using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ChannelETL.Extensions.DependencyInjection;

public abstract class PipelineGroup : IPipelineGroup
{
    //All pipeline types in this group
    private Dictionary<Type, PipelineBuilder> _pipelineBuilders = [];

    private Type[]? _cachedTypes;
    private Type[]? _cachedLoggerTypes;
    private bool _isInitialized;

    private void EnsureInitialized()
    {
        if (_isInitialized)
            return;

        _cachedTypes = [.. _pipelineBuilders.Keys];
        _cachedLoggerTypes = new Type[_cachedTypes.Length];

        for (int i = 0; i < _cachedTypes.Length; i++)
        {
            _cachedLoggerTypes[i] = typeof(ILogger<>).MakeGenericType(_cachedTypes[i]);
        }

        _isInitialized = true;
    }

    /// <summary>
    /// Adds a pipeline to the group.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the pipeline has already been added.</exception>
    protected IPipelineBuilder AddPipeline<TPipeline>() where TPipeline : IPipeline
    {
        _isInitialized = false;

        var builder = new PipelineBuilder(_pipelineBuilders.Keys);

        if (_pipelineBuilders.TryAdd(typeof(TPipeline), builder))
        {
            return builder;
        }

        throw new InvalidOperationException($"Pipeline of type {typeof(TPipeline).Name} has already been added.");
    }

    /// <summary>
    /// Runs all pipelines concurrently.
    /// </summary>
    public async Task RunAsync(PipelineGroupExecutionContext context)
    {
        EnsureInitialized();

        var count = _cachedTypes!.Length;
        if (count == 0)
            return;

        try
        {
            //Create a new scope for each run of the pipeline group
            using var scope = context.ScopeFactory.CreateScope();
            var provider = scope.ServiceProvider;

            //Initialize all pipelines using cached types
            var pipelines = new IPipeline[count];
            for (int i = 0; i < count; i++)
            {
                pipelines[i] = (IPipeline)provider.GetRequiredService(_cachedTypes[i]);
            }

            var tasks = new Task[count];

            for (int i = 0; i < count; i++)
            {
                var type = _cachedTypes[i];
                var builder = _pipelineBuilders[type];
                var logger = (ILogger)provider.GetRequiredService(_cachedLoggerTypes![i]);

                var parentPipelines = ResolveParentPipelines(builder.ParentPipelines, pipelines);
                var pipelineContext = new PipelineExecutionContext()
                {
                    ParentPipelines = parentPipelines,
                    Logger = logger,
                    Token = context.Token
                };

                tasks[i] = pipelines[i].RunAsync(pipelineContext);
            }

            await Task.WhenAll(tasks);
        }
        catch (AggregateException ae)
        {
            foreach (var ex in ae.Flatten().InnerExceptions)
            {
                context.Logger.LogError(ex, "An error occurred while running pipelines");
            }
        }
        catch (Exception e)
        {
            context.Logger.LogError(e, "An error occurred while running pipelines");
        }
    }

    private IEnumerable<IPipeline> ResolveParentPipelines(IEnumerable<Type> parentTypes, IPipeline[] pipelines)
    {
        foreach (var parentType in parentTypes)
        {
            for (int i = 0; i < _cachedTypes!.Length; i++)
            {
                if (_cachedTypes[i] == parentType)
                {
                    yield return pipelines[i];
                    break;
                }
            }
        }
    }
}
