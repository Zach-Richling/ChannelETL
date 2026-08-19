using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ChannelETL.Extensions.DependencyInjection;

public abstract class PipelineGroup : IPipelineGroup
{
    //All pipeline types in this group
    private Dictionary<Type, PipelineBuilder> _pipelineBuilders = [];

    /// <summary>
    /// Adds a pipeline to the group.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the pipeline has already been added.</exception>
    protected IPipelineBuilder AddPipeline<TPipeline>() where TPipeline : IPipeline
    {
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
        try
        {
            //Create a new scope for this run of the pipeline group
            using var scope = context.ScopeFactory.CreateScope();

            //Instantiate all pipelines in this group
            var pipelines = _pipelineBuilders
                .Select(x => (x.Key, Value: (IPipeline)scope.ServiceProvider.GetRequiredService(x.Key)))
                .ToDictionary(x => x.Key, x => x.Value);

            var tasks = new List<Func<Task>>();
            foreach (var (type, builder) in _pipelineBuilders)
            {
                var logger = (ILogger)scope.ServiceProvider.GetRequiredService(typeof(ILogger<>).MakeGenericType(type));
                var parentPipelines = pipelines.IntersectBy(builder.ParentPipelines, x => x.Key).Select(x => x.Value);
                var pipelineContext = new PipelineExecutionContext(parentPipelines, logger, context.Token);

                var pipeline = pipelines.TryGetValue(type, out var p)
                    ? p : throw new InvalidOperationException($"Pipeline of type {type.Name} not found in pipeline group");

                //Delay execution of the pipeline until all execution contexts have been created
                tasks.Add(() => pipeline.RunAsync(pipelineContext));
            }

            await Task.WhenAll(tasks.Select(x => x.Invoke()));
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
}
