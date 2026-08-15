using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ChannelETL.Tests;

internal static class TestComponents
{
    public static Source<T> CreateTestSource<T>(Func<IAsyncEnumerable<T>> items) => new(items());
    public static Transform<TIn, TOut> CreateTestTransform<TIn, TOut>(Func<TIn, CancellationToken, Task<TOut>> fn) => new(fn);
    public static Destination<T> CreateTestDestination<T>() => new();


    public class Source<T>(IAsyncEnumerable<T> items) : IPipelineSource<T>
    {
        public async IAsyncEnumerable<T> ProduceAsync([EnumeratorCancellation] CancellationToken token)
        {
            await foreach (var item in items)
            {
                token.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    public class Transform<TIn, TOut>(Func<TIn, CancellationToken, Task<TOut>> fn) : IPipelineTransformation<TIn, TOut>
    {
        public Task<TOut> TransformAsync(TIn item, CancellationToken token) => fn(item, token);
    }

    public class Destination<T> : IPipelineDestination<T>
    {
        private readonly ConcurrentQueue<T> _items = new();
        public IEnumerable<T> Items => [.. _items];

        public Task CompleteAsync(CancellationToken token) => Task.CompletedTask;

        public Task ConsumeAsync(T item, CancellationToken token)
        {
            _items.Enqueue(item);
            return Task.CompletedTask;
        }
    }
}
