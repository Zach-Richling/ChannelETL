using System.Threading.Channels;

namespace ChannelETL.Pipeline;

public class Pipeline<TSource, TDestination> : IPipeline<TSource, TDestination>
{
    public required IPipelineSource<TSource> Source { get; init; }
    public required IPipelineTransformation<TSource, TDestination> Transform { get; init; }
    public required IPipelineDestination<TDestination> Destination { get; init; }

    public async Task RunAsync(CancellationToken token)
    {
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

        await produceTask;
        sourceChannel.Writer.Complete();

        await transformTask;
        destinationChannel.Writer.Complete();

        await consumeTask;
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
