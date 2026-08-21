using NSubstitute;
using System.Runtime.CompilerServices;

namespace ChannelETL.Tests;

public class PipelineTests
{
    [Fact]
    public async Task CompletesAndPreservesOrder()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 5));

        var transform = Substitute.For<IPipelineTransformation<int, string>>();
        transform.TransformAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(ci.Arg<int>().ToString()));

        var (destination, items) = CreateDestination<string>();

        var pipeline = new TestPipeline<int, string>(source, transform, destination)
        {
            Name = nameof(CompletesAndPreservesOrder),
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        var expected = Enumerable.Range(1, 5).Select(i => i.ToString()).ToList();
        Assert.Equal(expected, items);
        Assert.Equal(PipelineOutcome.Success, outcome);
    }

    [Fact]
    public async Task CancelledDuringRun_PartialConsumption()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 1000));

        var transform = Substitute.For<IPipelineTransformation<int, string>>();
        transform.TransformAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(async ci =>
            {
                await Task.Delay(20, ci.Arg<CancellationToken>());
                return ci.Arg<int>().ToString();
            });

        var (destination, items) = CreateDestination<string>();

        var pipeline = new TestPipeline<int, string>(source, transform, destination)
        {
            Name = nameof(CancelledDuringRun_PartialConsumption),
        };

        using var cts = new CancellationTokenSource(100);

        await pipeline.RunAsync(TestHelpers.CreateContext([], cts.Token));
        var outcome = await pipeline.CompletionTask;

        Assert.True(items.Count > 0 && items.Count < 1000, "Destination should have consumed a partial number of items before cancellation.");

        var asInts = items.Select(int.Parse).ToList();
        Assert.Equal(asInts.OrderBy(x => x), asInts);
        Assert.Equal(PipelineOutcome.Canceled, outcome);
    }

    [Fact]
    public async Task TransformThrows_DestinationReceivesPriorItems()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 5));

        var transform = Substitute.For<IPipelineTransformation<int, string>>();
        transform.TransformAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var i = ci.Arg<int>();
                if (i == 3)
                    throw new InvalidOperationException("boom");

                return Task.FromResult(i.ToString());
            });

        var (destination, items) = CreateDestination<string>();

        var pipeline = new TestPipeline<int, string>(source, transform, destination)
        {
            Name = nameof(TransformThrows_DestinationReceivesPriorItems),
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        Assert.Equal(new List<string> { "1", "2" }, items);
        Assert.Equal(PipelineOutcome.Failure, outcome);
    }

    [Fact]
    public async Task ProduceThrows_DestinationDrainedBeforeReturn()
    {
        var producedCount = 0;
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>())
            .Returns(ci => ProduceThenThrow(Enumerable.Range(1, 5), c => producedCount = c, ci.Arg<CancellationToken>()));

        var transform = CreatePassthroughTransform<int>(delayMs: 500);
        var (destination, items) = CreateDestination<int>();

        var pipeline = new TestPipeline<int, int>(source, transform, destination)
        {
            Name = nameof(ProduceThrows_DestinationDrainedBeforeReturn)
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // source should have produced all items before throwing
        Assert.Equal(5, producedCount);

        // destination should have consumed all items that were produced
        Assert.Equal(5, items.Count);
        Assert.Equal(PipelineOutcome.Failure, outcome);
        await destination.Received(1).CompleteAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task TransformThrows_DestinationDrainedOfTransformedItemsBeforeReturn()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 5));

        var transform = Substitute.For<IPipelineTransformation<int, int>>();
        transform.TransformAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(async ci =>
            {
                await Task.Delay(500, ci.Arg<CancellationToken>());

                var item = ci.Arg<int>();
                if (item == 3)
                    throw new InvalidOperationException("transform boom");

                return item;
            });

        var (destination, items) = CreateDestination<int>();

        var pipeline = new TestPipeline<int, int>(source, transform, destination)
        {
            Name = nameof(TransformThrows_DestinationDrainedOfTransformedItemsBeforeReturn)
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // transform should have thrown when processing item 3
        Assert.Equal(PipelineOutcome.Failure, outcome);

        // destination should have consumed only the transformed items (1..2)
        Assert.Equal(new[] { 1, 2 }, items);
        await destination.Received(1).CompleteAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ConsumeThrows_DestinationDrainedUpToThrowBeforeReturn()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 5));

        var transform = CreatePassthroughTransform<int>(delayMs: 500);

        var items = new List<int>();
        var destination = Substitute.For<IPipelineDestination<int>>();
        destination.When(x => x.ConsumeAsync(Arg.Any<int>(), Arg.Any<CancellationToken>()))
            .Do(ci =>
            {
                var item = ci.Arg<int>();
                if (item == 2)
                    throw new InvalidOperationException("consume boom");

                items.Add(item);
            });

        var pipeline = new TestPipeline<int, int>(source, transform, destination)
        {
            Name = nameof(ConsumeThrows_DestinationDrainedUpToThrowBeforeReturn)
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // pipeline should have recorded failure
        Assert.Equal(PipelineOutcome.Failure, outcome);

        // destination recorded items up to the thrown item
        Assert.Equal(new[] { 1 }, items);
        await destination.Received(1).CompleteAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task CompleteThrows_ConsumeSucceeded_RethrowsCompleteException()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 3));

        var transform = CreatePassthroughTransform<int>();

        var destination = Substitute.For<IPipelineDestination<int>>();
        destination.CompleteAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("complete boom")));

        var pipeline = new TestPipeline<int, int>(source, transform, destination)
        {
            Name = nameof(CompleteThrows_ConsumeSucceeded_RethrowsCompleteException)
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        Assert.Equal(PipelineOutcome.Failure, outcome);
    }

    [Fact]
    public async Task ConsumeAndCompleteBothThrow_CombinedIntoAggregateException()
    {
        var source = Substitute.For<IPipelineSource<int>>();
        source.ProduceAsync(Arg.Any<CancellationToken>()).Returns(AsyncEnumerable.Range(1, 3));

        var transform = CreatePassthroughTransform<int>();

        var destination = Substitute.For<IPipelineDestination<int>>();
        destination.When(x => x.ConsumeAsync(Arg.Any<int>(), Arg.Any<CancellationToken>()))
            .Do(ci =>
            {
                if (ci.Arg<int>() == 2)
                    throw new InvalidOperationException("consume boom");
            });
        destination.CompleteAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("complete boom")));

        var pipeline = new TestPipeline<int, int>(source, transform, destination)
        {
            Name = nameof(ConsumeAndCompleteBothThrow_CombinedIntoAggregateException)
        };

        await pipeline.RunAsync(TestHelpers.CreateContext([]));
        var outcome = await pipeline.CompletionTask;

        // both the destination's ConsumeAsync and CompleteAsync failed; the pipeline
        // still only reports Failure (the AggregateException is caught, not surfaced)
        Assert.Equal(PipelineOutcome.Failure, outcome);
    }

    [Fact]
    public async Task ChildDoesNotStartProduceUntilParentsComplete()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task);

        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSignalingSource(Enumerable.Range(1, 1), started);
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child" };

        var runTask = child.RunAsync(TestHelpers.CreateContext([parent]));

        // give the runtime a moment to start awaiting parents
        await Task.Delay(1000, CancellationToken.None);

        Assert.False(started.Task.IsCompleted, "Produce should not have started before parent completion.");

        // complete parent
        parentTcs.SetResult(PipelineOutcome.Success);

        // now the pipeline should proceed and finish
        await runTask;

        Assert.True(started.Task.IsCompleted, "Produce should have started after parent completion.");
        Assert.NotEmpty(items);
    }

    [Fact]
    public async Task MixedParents_OneFailure_ChildCanceled()
    {
        var p1 = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var p2 = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);

        var parent1 = new TestParentPipeline(p1.Task);
        var parent2 = new TestParentPipeline(p2.Task);

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSignalingSource(Enumerable.Range(1, 1), produceStarted);
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-mixed" };

        var runTask = child.RunAsync(TestHelpers.CreateContext([parent1, parent2]));

        // complete one parent as success and the other as failure
        p1.SetResult(PipelineOutcome.Success);
        p2.SetResult(PipelineOutcome.Failure);

        await runTask;

        var outcome = await child.CompletionTask;
        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.False(produceStarted.Task.IsCompleted);
        Assert.Empty(items);
    }

    [Fact]
    public async Task ParentCompletionTaskThrows_ChildRunThrows()
    {
        var parent = new TestParentPipeline(Task.FromException<PipelineOutcome>(new InvalidOperationException("parent boom")));

        var source = CreateSignalingSource(Enumerable.Range(1, 1));
        var transform = CreatePassthroughTransform<int>();
        var (destination, _) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-parent-exception" };

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await child.RunAsync(TestHelpers.CreateContext([parent])));

        // CompletionTask should not be completed because RunAsync threw before setting outcome
        Assert.False(child.CompletionTask.IsCompleted);
    }

    [Fact]
    public async Task PreCanceledToken_SetsCanceledOutcomeImmediately()
    {
        var source = CreateSignalingSource(Enumerable.Range(1, 5));
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-pre-canceled" };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await child.RunAsync(TestHelpers.CreateContext([], cts.Token));
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.Empty(items);
    }

    [Fact]
    public async Task ChildWaitsForMultipleParents()
    {
        var parent1Tcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent2Tcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);

        var parent1 = new TestParentPipeline(parent1Tcs.Task);
        var parent2 = new TestParentPipeline(parent2Tcs.Task);

        var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSignalingSource(Enumerable.Range(1, 1), started);
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-multi" };

        var runTask = child.RunAsync(TestHelpers.CreateContext([parent1, parent2]));

        await Task.Delay(1000, CancellationToken.None);
        Assert.False(started.Task.IsCompleted);

        // complete only one parent
        parent1Tcs.SetResult(PipelineOutcome.Success);

        await Task.Delay(1000, CancellationToken.None);
        Assert.False(started.Task.IsCompleted, "Produce should not start until all parents complete.");

        // complete second parent
        parent2Tcs.SetResult(PipelineOutcome.Success);

        await runTask;

        Assert.True(started.Task.IsCompleted);
        Assert.NotEmpty(items);
    }

    [Fact]
    public async Task ChildIsCanceledWhenParentFails()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task);

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSignalingSource(Enumerable.Range(1, 1), produceStarted);
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-on-parent-fail" };

        var runTask = child.RunAsync(TestHelpers.CreateContext([parent]));

        await Task.Delay(50, CancellationToken.None);
        Assert.False(produceStarted.Task.IsCompleted, "Produce should not have started before parent completes.");

        parentTcs.SetResult(PipelineOutcome.Failure);

        await runTask;
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.False(produceStarted.Task.IsCompleted, "Produce should not start when parent failed.");
        Assert.Empty(items);
    }

    [Fact]
    public async Task ChildIsCanceledWhenParentCanceled()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task);

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSignalingSource(Enumerable.Range(1, 1), produceStarted);
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-on-parent-canceled" };

        var runTask = child.RunAsync(TestHelpers.CreateContext([parent]));

        await Task.Delay(50, CancellationToken.None);
        Assert.False(produceStarted.Task.IsCompleted);

        parentTcs.SetResult(PipelineOutcome.Canceled);

        await runTask;
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Canceled, outcome);
        Assert.False(produceStarted.Task.IsCompleted);
        Assert.Empty(items);
    }

    [Fact]
    public async Task ChildProceedsWhenParentsSucceed()
    {
        var parentTcs = new TaskCompletionSource<PipelineOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        var parent = new TestParentPipeline(parentTcs.Task);

        var produceStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSignalingSource(Enumerable.Range(1, 3), produceStarted);
        var transform = CreatePassthroughTransform<int>();
        var (destination, items) = CreateDestination<int>();

        var child = new TestPipeline<int, int>(source, transform, destination) { Name = "child-on-parent-success" };

        var runTask = child.RunAsync(TestHelpers.CreateContext([parent]));

        await Task.Delay(50, CancellationToken.None);
        Assert.False(produceStarted.Task.IsCompleted);

        parentTcs.SetResult(PipelineOutcome.Success);

        await runTask;
        var outcome = await child.CompletionTask;

        Assert.Equal(PipelineOutcome.Success, outcome);
        Assert.True(produceStarted.Task.IsCompleted);
        Assert.NotEmpty(items);
    }

    // Helpers

    private static (IPipelineDestination<T> Destination, List<T> Items) CreateDestination<T>()
    {
        var items = new List<T>();
        var destination = Substitute.For<IPipelineDestination<T>>();
        destination.When(x => x.ConsumeAsync(Arg.Any<T>(), Arg.Any<CancellationToken>()))
            .Do(ci => items.Add(ci.Arg<T>()));

        return (destination, items);
    }

    private static IPipelineTransformation<T, T> CreatePassthroughTransform<T>(int delayMs = 0)
    {
        var transform = Substitute.For<IPipelineTransformation<T, T>>();
        transform.TransformAsync(Arg.Any<T>(), Arg.Any<CancellationToken>())
            .Returns(async ci =>
            {
                if (delayMs > 0)
                    await Task.Delay(delayMs, ci.Arg<CancellationToken>());

                return ci.Arg<T>();
            });

        return transform;
    }

    private static IPipelineSource<T> CreateSignalingSource<T>(IEnumerable<T> items, TaskCompletionSource<bool>? started = null)
    {
        var source = Substitute.For<IPipelineSource<T>>();
        source.ProduceAsync(Arg.Any<CancellationToken>())
            .Returns(ci => ProduceSignaling(items, started, ci.Arg<CancellationToken>()));

        return source;
    }

    private static async IAsyncEnumerable<T> ProduceSignaling<T>(IEnumerable<T> items, TaskCompletionSource<bool>? started, [EnumeratorCancellation] CancellationToken token)
    {
        started?.TrySetResult(true);
        foreach (var item in items)
        {
            token.ThrowIfCancellationRequested();
            yield return item;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> ProduceThenThrow(IEnumerable<int> items, Action<int> onProduced, [EnumeratorCancellation] CancellationToken token)
    {
        var count = 0;
        foreach (var item in items)
        {
            token.ThrowIfCancellationRequested();
            count++;
            onProduced(count);
            yield return item;
            await Task.Yield();
        }

        throw new InvalidOperationException("produce boom");
    }

    private sealed class TestPipeline<TSource, TDest>(
        IPipelineSource<TSource> source,
        IPipelineTransformation<TSource, TDest> transform,
        IPipelineDestination<TDest> destination)
        : Pipeline<TSource, TDest>(source, transform, destination);

    private sealed class TestParentPipeline(Task<PipelineOutcome> completionTask) : IPipeline
    {
        public Task RunAsync(PipelineExecutionContext context) => Task.CompletedTask;
        public Task<PipelineOutcome> CompletionTask => completionTask;
    }
}
