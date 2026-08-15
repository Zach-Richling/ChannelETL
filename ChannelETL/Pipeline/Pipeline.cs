using System.Threading.Channels;

namespace ChannelETL;

public class Pipeline<TSource, TDestination> : IPipeline<TSource, TDestination>
{
    public required IPipelineSource<TSource> Source { get; init; }
    public required IPipelineTransformation<TSource, TDestination> Transform { get; init; }
    public required IPipelineDestination<TDestination> Destination { get; init; }
    public required string Name { get; init; }
    public required IEnumerable<IPipeline> ParentPipelines { get; init; }

    private readonly TaskCompletionSource<PipelineOutcome> _tcs = new();
    public Task<PipelineOutcome> CompletionTask => _tcs.Task;

    private PipelineOutcome _outcome = PipelineOutcome.Success;

    internal Pipeline() { }

    public async Task RunAsync(CancellationToken token)
    {
        var parentOutcomes = await Task.WhenAll(ParentPipelines.Select(x => x.CompletionTask));

        if (token.IsCancellationRequested || parentOutcomes.Any(x => x != PipelineOutcome.Success))
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

        var produceTask = ProduceAsync(sourceChannel.Writer, token);
        var transformTask = TransformAsync(sourceChannel.Reader, destinationChannel.Writer, token);
        var consumeTask = ConsumeAsync(destinationChannel.Reader, token);

        try
        {
            await produceTask;
        }
        catch
        {
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
        catch
        {
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
        catch
        {
            _outcome = PipelineOutcome.Failure;
        }

        _tcs.SetResult(token.IsCancellationRequested ? PipelineOutcome.Canceled : _outcome);
    }

    private async Task ProduceAsync(ChannelWriter<TSource> writer, CancellationToken token)
    {
        await foreach (var record in Source.ProduceAsync(token))
        {
            await writer.WriteAsync(record, token);
        }
    }

    private async Task TransformAsync(ChannelReader<TSource> reader, ChannelWriter<TDestination> writer, CancellationToken token)
    {
        await foreach (var record in reader.ReadAllAsync(token))
        {
            var transformed = await Transform.TransformAsync(record, token);
            await writer.WriteAsync(transformed, token);
        }
    }

    private async Task ConsumeAsync(ChannelReader<TDestination> reader, CancellationToken token)
    {
        await foreach (var record in reader.ReadAllAsync(token))
        {
            await Destination.ConsumeAsync(record, token);
        }
    }
}
