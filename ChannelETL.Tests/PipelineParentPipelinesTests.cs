using ChannelETL.Pipeline;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ChannelETL.Tests;

public class PipelineParentPipelinesTests
{
    [Fact]
    public async Task ChildDoesNotStartProduceUntilParentsComplete()
    {
        var parentTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task, "parent");

        var childTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), childTcs);
        var transform = new TestTransform<int, int>(async (i, ct) => { await Task.Yield(); return i; });
        var destination = new TestDestination<int>();

        var child = new Pipeline<int, int>
        {
            Name = "child",
            ParentPipelines = [parent],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        // give the runtime a moment to start awaiting parents
        await Task.Delay(1000);

        Assert.False(childTcs.Task.IsCompleted, "Produce should not have started before parent completion.");

        // complete parent
        parentTcs.SetResult();

        // now the pipeline should proceed and finish
        await runTask;

        Assert.True(childTcs.Task.IsCompleted, "Produce should have started after parent completion.");
        Assert.NotEmpty(destination.Items);
    }

    [Fact]
    public async Task ChildWaitsForMultipleParents()
    {
        var parent1Cts = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent2Cts = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var parent1 = new TestParentPipeline(parent1Cts.Task, "p1");
        var parent2 = new TestParentPipeline(parent2Cts.Task, "p2");

        var childCts = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), childCts);
        var transform = new TestTransform<int, int>(async (i, ct) => { await Task.Yield(); return i; });
        var destination = new TestDestination<int>();

        var child = new Pipeline<int, int>
        {
            Name = "child-multi",
            ParentPipelines = [parent1, parent2],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        await Task.Delay(1000);
        Assert.False(childCts.Task.IsCompleted);

        // complete only one parent
        parent1Cts.SetResult();
        await Task.Delay(1000);
        Assert.False(childCts.Task.IsCompleted, "Produce should not start until all parents complete.");

        // complete second parent
        parent2Cts.SetResult();

        await runTask;

        Assert.True(childCts.Task.IsCompleted);
        Assert.NotEmpty(destination.Items);
    }

    private class TestParentPipeline(Task completionTask, string name) : IPipeline
    {
        public string Name { get; } = name;
        public IEnumerable<IPipeline> ParentPipelines => [];
        public Task RunAsync(CancellationToken token) => Task.CompletedTask;
        public Task CompletionTask => completionTask;
    }

    // reuse small test helpers similar to PipelineTests
    private class TestSource<T>(IEnumerable<T> items, TaskCompletionSource<bool>? started = null) : IPipelineSource<T>
    {
        public async IAsyncEnumerable<T> ProduceAsync([EnumeratorCancellation] CancellationToken token)
        {
            started?.TrySetResult(true);
            foreach (var item in items)
            {
                token.ThrowIfCancellationRequested();
                yield return item;
            }
            await Task.CompletedTask;
        }
    }

    private class TestTransform<TIn, TOut>(Func<TIn, CancellationToken, Task<TOut>> fn) : IPipelineTransformation<TIn, TOut>
    {
        public Task<TOut> TransformAsync(TIn item, CancellationToken token) => fn(item, token);
    }

    private class TestDestination<T> : IPipelineDestination<T>
    {
        private readonly ConcurrentQueue<T> _items = new();
        public IEnumerable<T> Items => _items.ToArray();
        public Task ConsumeAsync(T item, CancellationToken token)
        {
            _items.Enqueue(item);
            return Task.CompletedTask;
        }
    }
}
