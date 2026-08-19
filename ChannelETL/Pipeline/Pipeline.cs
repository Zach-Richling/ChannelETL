using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace ChannelETL;

public abstract class Pipeline<TSource, TDestination>(
    IPipelineSource<TSource> source,
    IPipelineTransformation<TSource, TDestination> transform,
    IPipelineDestination<TDestination> destination)
    : IPipeline
{
    public string Name { get; init; } = "";

    private readonly TaskCompletionSource<PipelineOutcome> _tcs = new();
    public Task<PipelineOutcome> CompletionTask => _tcs.Task;

    private PipelineOutcome _outcome = PipelineOutcome.Success;

    /// <summary>
    /// Runs the pipeline asynchronously, logging information and errors.
    /// </summary>
    public async Task RunAsync(PipelineExecutionContext context)
    {
        //TODO: Retries, Parallelism, and Deadletter
        var parentOutcomes = await Task.WhenAll(context.ParentPipelines.Select(x => x.CompletionTask));

        if (context.Token.IsCancellationRequested || parentOutcomes.Any(x => x != PipelineOutcome.Success))
        {
            _tcs.SetResult(PipelineOutcome.Canceled);
            return;
        }

        var sourceChannel = Channel.CreateBounded<TSource>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        var destinationChannel = Channel.CreateBounded<TDestination>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true
        });

        context.Logger.LogInformation("Starting pipeline execution...");
        var produceTask = ProduceAsync(sourceChannel.Writer, context.Token);
        var transformTask = TransformAsync(sourceChannel.Reader, destinationChannel.Writer, context.Token);
        var consumeTask = ConsumeAsync(destinationChannel.Reader, context.Token);

        try
        {
            await produceTask;
        }
        catch (OperationCanceledException) { }
        catch (Exception e)
        {
            context.Logger.LogError(e, "An error occurred while producing data.");
            _outcome = PipelineOutcome.Failure;
        }
        finally
        {
            sourceChannel.Writer.Complete();
        }

        try
        {
            await transformTask;
        }
        catch (OperationCanceledException) { }
        catch (Exception e)
        {
            context.Logger.LogError(e, "An error occurred while transforming data.");
            _outcome = PipelineOutcome.Failure;
        }
        finally
        {
            destinationChannel.Writer.Complete();
        }

        try
        {
            await consumeTask;
        }
        catch (OperationCanceledException) { }
        catch (Exception e)
        {
            context.Logger.LogError(e, "An error occurred while consuming data.");
            _outcome = PipelineOutcome.Failure;
        }

        _outcome = context.Token.IsCancellationRequested ? PipelineOutcome.Canceled : _outcome;

        context.Logger.LogInformation("Pipeline execution completed with status: {PipelineOutcome}", _outcome);
        _tcs.SetResult(_outcome);
    }

    private async Task ProduceAsync(ChannelWriter<TSource> writer, CancellationToken token)
    {
        await foreach (var record in source.ProduceAsync(token))
        {
            await writer.WriteAsync(record, token);
        }
    }

    private async Task TransformAsync(ChannelReader<TSource> reader, ChannelWriter<TDestination> writer, CancellationToken token)
    {
        await foreach (var record in reader.ReadAllAsync(token))
        {
            var transformed = await transform.TransformAsync(record, token);
            await writer.WriteAsync(transformed, token);
        }
    }

    private async Task ConsumeAsync(ChannelReader<TDestination> reader, CancellationToken token)
    {
        var destException = default(Exception);

        try
        {
            await foreach (var record in reader.ReadAllAsync(token))
            {
                await destination.ConsumeAsync(record, token);
            }
        }
        catch (Exception e)
        {
            destException = e;
        }

        try
        {
            await destination.CompleteAsync(token);
        }
        catch (Exception e)
        {
            if (destException != null)
                throw new AggregateException("Destination and completion both failed.", destException, e);

            throw;
        }

        if (destException != null)
            throw destException;
    }
}
