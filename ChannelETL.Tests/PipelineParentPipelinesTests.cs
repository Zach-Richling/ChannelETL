using Microsoft.Extensions.Logging;
using NSubstitute;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ChannelETL.Tests;

public class PipelineParentPipelinesTests
{
    [Fact]
    public async Task ChildDoesNotStartProduceUntilParentsComplete()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task, "parent");

        var childTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), childTcs);
        var transform = new TestTransform<int, int>(async (i, ct) => { await Task.Yield(); return i; });
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child",
            ParentPipelines = [parent],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        // give the runtime a moment to start awaiting parents
        await Task.Delay(1000, CancellationToken.None);

        Assert.False(childTcs.Task.IsCompleted, "Produce should not have started before parent completion.");

        // complete parent
        parentTcs.SetResult(PipelineOutcome.Success);

        // now the pipeline should proceed and finish
        await runTask;

        Assert.True(childTcs.Task.IsCompleted, "Produce should have started after parent completion.");
        Assert.NotEmpty(destination.Items);
    }

    [Fact]
    public async Task MixedParents_OneFailure_ChildCanceled()
    {
        var p1 = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var p2 = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);

        var parent1 = new TestParentPipeline(p1.Task, "p1");
        var parent2 = new TestParentPipeline(p2.Task, "p2");

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), produceStarted);
        var transform = new TestTransform<int, int>((i, ct) => Task.FromResult(i));
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-mixed",
            ParentPipelines = [parent1, parent2],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        // complete one parent as success and the other as failure
        p1.SetResult(PipelineOutcome.Success);
        p2.SetResult(PipelineOutcome.Failure);

        await runTask;

        var outcome = await child.CompletionTask;
        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.False(produceStarted.Task.IsCompleted);
        Assert.Empty(destination.Items);
    }

    [Fact]
    public async Task ParentCompletionTaskThrows_ChildRunThrows()
    {
        var parent = new TestParentPipeline(Task.FromException<PipelineOutcome>(new InvalidOperationException("parent boom")), "bad");

        var source = new TestSource<int>(Enumerable.Range(1, 1));
        var transform = new TestTransform<int, int>((i, ct) => Task.FromResult(i));
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-parent-exception",
            ParentPipelines = [parent],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await child.RunAsync(CancellationToken.None));
        // CompletionTask should not be completed because RunAsync threw before setting outcome
        Assert.False(child.CompletionTask.IsCompleted);
    }

    [Fact]
    public async Task PreCanceledToken_SetsCanceledOutcomeImmediately()
    {
        var source = new TestSource<int>(Enumerable.Range(1, 5));
        var transform = new TestTransform<int, int>((i, ct) => Task.FromResult(i));
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-pre-canceled",
            ParentPipelines = [],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await child.RunAsync(cts.Token);
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.Empty(destination.Items);
    }

    [Fact]
    public async Task ChildWaitsForMultipleParents()
    {
        var parent1Cts = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent2Cts = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);

        var parent1 = new TestParentPipeline(parent1Cts.Task, "p1");
        var parent2 = new TestParentPipeline(parent2Cts.Task, "p2");

        var childCts = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), childCts);
        var transform = new TestTransform<int, int>(async (i, ct) => { await Task.Yield(); return i; });
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-multi",
            ParentPipelines = [parent1, parent2],
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        await Task.Delay(1000, CancellationToken.None);
        Assert.False(childCts.Task.IsCompleted);

        // complete only one parent
        parent1Cts.SetResult(PipelineOutcome.Success);

        await Task.Delay(1000, CancellationToken.None);
        Assert.False(childCts.Task.IsCompleted, "Produce should not start until all parents complete.");

        // complete second parent
        parent2Cts.SetResult(PipelineOutcome.Success);

        await runTask;

        Assert.True(childCts.Task.IsCompleted);
        Assert.NotEmpty(destination.Items);
    }

    [Fact]
    public async Task ChildIsCanceledWhenParentFails()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task, "parent-fail");

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), produceStarted);
        var transform = new TestTransform<int, int>(async (i, ct) => { await Task.Yield(); return i; });
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-on-parent-fail",
            ParentPipelines = new[] { parent },
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        await Task.Delay(50, CancellationToken.None);
        Assert.False(produceStarted.Task.IsCompleted, "Produce should not have started before parent completes.");

        parentTcs.SetResult(PipelineOutcome.Failure);

        await runTask;
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.False(produceStarted.Task.IsCompleted, "Produce should not start when parent failed.");
        Assert.Empty(destination.Items);
    }

    [Fact]
    public async Task ChildIsCanceledWhenParentCanceled()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task, "parent-canceled");

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 1), produceStarted);
        var transform = new TestTransform<int, int>((i, ct) => Task.FromResult(i));
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-on-parent-canceled",
            ParentPipelines = new[] { parent },
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        await Task.Delay(50, CancellationToken.None);
        Assert.False(produceStarted.Task.IsCompleted);

        parentTcs.SetResult(PipelineOutcome.Canceled);

        await runTask;
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.False(produceStarted.Task.IsCompleted);
        Assert.Empty(destination.Items);
    }

    [Fact]
    public async Task ChildProceedsWhenParentsSucceed()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task, "parent-success");

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new TestSource<int>(Enumerable.Range(1, 3), produceStarted);
        var transform = new TestTransform<int, int>((i, ct) => Task.FromResult(i));
        var destination = new TestDestination<int>();
        var logger = Substitute.For<ILogger<Pipeline<int, int>>>();

        var child = new Pipeline<int, int>(logger)
        {
            Name = "child-on-parent-success",
            ParentPipelines = new[] { parent },
            Source = source,
            Transform = transform,
            Destination = destination
        };

        var runTask = child.RunAsync(CancellationToken.None);

        await Task.Delay(50, CancellationToken.None);
        Assert.False(produceStarted.Task.IsCompleted);

        parentTcs.SetResult(PipelineOutcome.Success);

        await runTask;
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Success, outcome);
        Assert.True(produceStarted.Task.IsCompleted);
        Assert.NotEmpty(destination.Items);
    }

    private class TestParentPipeline(Task<PipelineOutcome> completionTask, string name) : IPipeline
    {
        public string Name { get; } = name;
        public IEnumerable<IPipeline> ParentPipelines => [];
        public Task RunAsync(CancellationToken token) => Task.CompletedTask;
        public Task<PipelineOutcome> CompletionTask => completionTask;
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

        public Task CompleteAsync(CancellationToken token) => Task.CompletedTask;
    }
}
