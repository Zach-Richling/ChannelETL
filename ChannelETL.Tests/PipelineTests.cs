using ChannelETL.Pipeline;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ChannelETL.Tests
{
    public class PipelineTests
    {
        [Fact]
        public async Task CompletesAndPreservesOrder()
        {
            var source = new TestSource<int>(Enumerable.Range(1, 5));
            var transform = new TestTransform<int, string>(async (i, ct) => i.ToString());
            var destination = new TestDestination<string>();

            var pipeline = new Pipeline<int, string>
            {
                Source = source,
                Transform = transform,
                Destination = destination
            };

            await pipeline.RunAsync(CancellationToken.None);

            var consumed = destination.Items.ToList();
            var expected = Enumerable.Range(1, 5).Select(i => i.ToString()).ToList();
            Assert.Equal(expected, consumed);
        }

        [Fact]
        public async Task CancelledDuringRun_ThrowsOperationCanceledAndPartialConsumption()
        {
            var source = new TestSource<int>(GenerateSequence(1, 1000));
            var transform = new TestTransform<int, string>(async (i, ct) =>
            {
                await Task.Delay(20, ct);
                return i.ToString();
            });
            var destination = new TestDestination<string>();

            var pipeline = new Pipeline<int, string>
            {
                Source = source,
                Transform = transform,
                Destination = destination
            };

            using var cts = new CancellationTokenSource(100);

            await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            {
                await pipeline.RunAsync(cts.Token);
            });

            var consumed = destination.Items.ToList();
            Assert.True(consumed.Count > 0 && consumed.Count < 1000, "Destination should have consumed a partial number of items before cancellation.");

            var asInts = consumed.Select(int.Parse).ToList();
            Assert.Equal(asInts.OrderBy(x => x), asInts);
        }

        [Fact]
        public async Task TransformThrows_RunThrowsAndDestinationReceivesPriorItems()
        {
            var source = new TestSource<int>(Enumerable.Range(1, 5));
            var transform = new TestTransform<int, string>(async (i, ct) =>
            {
                if (i == 3) throw new InvalidOperationException("boom");
                await Task.Yield();
                return i.ToString();
            });
            var destination = new TestDestination<string>();

            var pipeline = new Pipeline<int, string>
            {
                Source = source,
                Transform = transform,
                Destination = destination
            };

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pipeline.RunAsync(CancellationToken.None));
            Assert.Equal("boom", ex.Message);

            var consumed = destination.Items.ToList();
            var expectedPrior = new List<string> { "1", "2" };
            Assert.Equal(expectedPrior, consumed);
        }

        private static IEnumerable<int> GenerateSequence(int start, int count)
        {
            for (int i = start; i < start + count; i++) yield return i;
        }
    }

    internal class TestSource<T>(IEnumerable<T> items) : IPipelineSource<T>
    {
        public async IAsyncEnumerable<T> ProduceAsync([EnumeratorCancellation] CancellationToken token)
        {
            foreach (var item in items)
            {
                token.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    internal class TestTransform<TIn, TOut>(Func<TIn, CancellationToken, Task<TOut>> fn) : IPipelineTransformation<TIn, TOut>
    {
        public Task<TOut> TransformAsync(TIn item, CancellationToken token) => fn(item, token);
    }

    internal class TestDestination<T> : IPipelineDestination<T>
    {
        private readonly ConcurrentQueue<T> _items = new();
        public IEnumerable<T> Items => [.. _items];
        public Task ConsumeAsync(T item, CancellationToken token)
        {
            _items.Enqueue(item);
            return Task.CompletedTask;
        }
    }
}
