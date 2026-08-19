using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ChannelETL.Tests;

public class PipelineChannelDrainTests
{
    private static PipelineExecutionContext CreateContext(IEnumerable<IPipeline> parentPipelines, CancellationToken? token = null) => new()
    {
        ParentPipelines = parentPipelines,
        Logger = NullLogger.Instance,
        Token = token ?? CancellationToken.None
    };

    [Fact]
    public async Task ProduceThrows_DestinationDrainedBeforeReturn()
    {
        var source = new ThrowingSource<int>(Enumerable.Range(1, 5), throwAfterYield: true);
        var transform = new PassthroughTransform<int>(delayMs: 500);
        var destination = new RecordingDestination<int>();

        var pipeline = new TestPipeline(source, transform, destination)
        {
            Name = nameof(ProduceThrows_DestinationDrainedBeforeReturn)
        };

        await pipeline.RunAsync(CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // source should have produced all items before throwing
        Assert.Equal(5, source.Produced);

        // destination should have consumed all items that were produced
        Assert.Equal(5, destination.Items.Count());
        Assert.Equal(PipelineOutcome.Failure, outcome);
        Assert.True(destination.CalledComplete);
    }

    [Fact]
    public async Task TransformThrows_DestinationDrainedOfTransformedItemsBeforeReturn()
    {
        var source = new SimpleSource<int>(Enumerable.Range(1, 5));
        var transform = new ThrowingTransform<int, int>(throwOn: 3, delayMs: 500);
        var destination = new RecordingDestination<int>();

        var pipeline = new TestPipeline(source, transform, destination)
        {
            Name = nameof(TransformThrows_DestinationDrainedOfTransformedItemsBeforeReturn)
        };

        await pipeline.RunAsync(CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // transform should have thrown when processing item 3
        Assert.Equal(PipelineOutcome.Failure, outcome);

        // destination should have consumed only the transformed items (1..2)
        Assert.Equal(new[] { 1, 2 }, destination.Items.ToArray());
        Assert.True(destination.CalledComplete);
    }

    [Fact]
    public async Task ConsumeThrows_DestinationDrainedUpToThrowBeforeReturn()
    {
        var source = new SimpleSource<int>(Enumerable.Range(1, 5));
        var transform = new PassthroughTransform<int>(delayMs: 500);
        var destination = new ThrowingDestination<int>(throwOnConsume: 2);

        var pipeline = new TestPipeline(source, transform, destination)
        {
            Name = nameof(ConsumeThrows_DestinationDrainedUpToThrowBeforeReturn)
        };

        await pipeline.RunAsync(CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // pipeline should have recorded failure
        Assert.Equal(PipelineOutcome.Failure, outcome);

        // destination recorded items up to the thrown item
        Assert.Equal(new[] { 1 }, destination.ConsumedSoFar.ToArray());
        Assert.True(destination.CalledComplete);
    }

    // Helpers
    public class TestPipeline(
        IPipelineSource<int> source,
        IPipelineTransformation<int, int> transform,
        IPipelineDestination<int> destination)
        : Pipeline<int, int>(source, transform, destination);

    private class SimpleSource<T>(IEnumerable<T> items) : IPipelineSource<T>
    {
        public int Produced;
        private readonly IEnumerable<T> _items = items;
        public async IAsyncEnumerable<T> ProduceAsync([EnumeratorCancellation] CancellationToken token)
        {
            foreach (var it in _items)
            {
                token.ThrowIfCancellationRequested();
                Produced++;
                yield return it;
                await Task.Yield();
            }
        }
    }

    private class ThrowingSource<T>(IEnumerable<T> items, bool throwAfterYield) : IPipelineSource<T>
    {
        public int Produced;
        private readonly IEnumerable<T> _items = items;
        private readonly bool _throwAfterYield = throwAfterYield;
        public async IAsyncEnumerable<T> ProduceAsync([EnumeratorCancellation] CancellationToken token)
        {
            foreach (var it in _items)
            {
                token.ThrowIfCancellationRequested();
                Produced++;
                yield return it;
                await Task.Yield();
            }

            if (_throwAfterYield)
            {
                throw new InvalidOperationException("produce boom");
            }
        }
    }

    private class PassthroughTransform<T>(int delayMs = 0) : IPipelineTransformation<T, T>
    {
        public async Task<T> TransformAsync(T item, CancellationToken token)
        {
            if (delayMs > 0)
                await Task.Delay(delayMs, token);
            return item;
        }
    }

    private class ThrowingTransform<TIn, TOut>(TIn throwOn, int delayMs = 0) : IPipelineTransformation<TIn, TOut>
    {
        private readonly bool _hasThrowOn = true;

        public async Task<TOut> TransformAsync(TIn item, CancellationToken token)
        {
            if (delayMs > 0)
                await Task.Delay(delayMs, token);

            if (_hasThrowOn && EqualityComparer<TIn>.Default.Equals(item, throwOn))
                throw new InvalidOperationException("transform boom");

            return (TOut)(object)item!;
        }
    }

    private class RecordingDestination<T> : IPipelineDestination<T>
    {
        private readonly ConcurrentQueue<T> _items = new();
        public IEnumerable<T> Items => _items.ToArray();
        public bool CalledComplete { get; private set; } = false;

        public Task CompleteAsync(CancellationToken token)
        {
            CalledComplete = true;
            return Task.CompletedTask;
        }

        public Task ConsumeAsync(T item, CancellationToken token)
        {
            _items.Enqueue(item);
            return Task.CompletedTask;
        }
    }

    private class ThrowingDestination<T> : IPipelineDestination<T>
    {
        private readonly ConcurrentQueue<T> _consumed = new();
        private readonly T _throwOn;
        private readonly bool _hasThrowOn;
        public IEnumerable<T> ConsumedSoFar => _consumed.ToArray();

        public bool CalledComplete { get; private set; } = false;

        public ThrowingDestination(T throwOnConsume)
        {
            _throwOn = throwOnConsume;
            _hasThrowOn = true;
        }

        public Task CompleteAsync(CancellationToken token)
        {
            CalledComplete = true;
            return Task.CompletedTask;
        }

        public Task ConsumeAsync(T item, CancellationToken token)
        {
            if (_hasThrowOn && EqualityComparer<T>.Default.Equals(item, _throwOn))
                throw new InvalidOperationException("consume boom");

            _consumed.Enqueue(item);
            return Task.CompletedTask;
        }
    }
}
