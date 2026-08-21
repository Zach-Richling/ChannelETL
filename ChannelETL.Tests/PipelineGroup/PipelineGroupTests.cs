using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChannelETL.Tests;

public class PipelineGroupTests
{
    public ServiceProvider Services { get; init; }
    public PipelineGroupTests()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<SharedState>();
        services.AddPipelinesFromAssembly(typeof(PipelineGroupTests).Assembly);
        Services = services.BuildServiceProvider();
    }

    [Fact]
    public async Task RunAsync_RunsAllPipelines_And_ChildWaitsForParentCompletion()
    {
        var group = new TestPipelineGroup();

        var context = new PipelineGroupExecutionContext()
        {
            ScopeFactory = Services.GetRequiredService<IServiceScopeFactory>(),
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        };

        await group.RunAsync(context);

        var state = Services.GetRequiredService<SharedState>();
        Assert.Equal(2, state.Events.Count);

        // Parent must have recorded before child
        Assert.Equal("parent", state.Events[0]);
        Assert.Equal("child", state.Events[1]);
    }

    [Fact]
    public async Task RunAsync_ChildWithManyParents_WaitsForAllParents()
    {
        var group = new ManyParentsPipelineGroup();

        var context = new PipelineGroupExecutionContext()
        {
            ScopeFactory = Services.GetRequiredService<IServiceScopeFactory>(),
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        };

        await group.RunAsync(context);

        var state = Services.GetRequiredService<SharedState>();
        // Three parents + one child
        Assert.Equal(4, state.Events.Count);

        // Child should be last because it waits for all parents
        Assert.Equal("child-many", state.Events.Last());

        var firstThree = state.Events.Take(3).ToList();
        Assert.Contains(nameof(ParentPipelineA), firstThree);
        Assert.Contains(nameof(ParentPipelineB), firstThree);
        Assert.Contains(nameof(ParentPipelineC), firstThree);
    }

    [Fact]
    public async Task RunAsync_CalledTwiceOnSameGroup_RunsPipelinesBothTimes()
    {
        var group = new TestPipelineGroup();

        var context = new PipelineGroupExecutionContext()
        {
            ScopeFactory = Services.GetRequiredService<IServiceScopeFactory>(),
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        };

        // second call exercises the already-initialized short-circuit in EnsureInitialized
        await group.RunAsync(context);
        await group.RunAsync(context);

        var state = Services.GetRequiredService<SharedState>();
        Assert.Equal(4, state.Events.Count);
    }

    [Fact]
    public void AddPipeline_SameTypeTwice_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => new DuplicatePipelineGroup());
    }

    [Fact]
    public async Task RunAsync_NoPipelinesAdded_CompletesWithoutRunningAnything()
    {
        var group = new EmptyPipelineGroup();

        var context = new PipelineGroupExecutionContext()
        {
            ScopeFactory = Services.GetRequiredService<IServiceScopeFactory>(),
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        };

        await group.RunAsync(context);

        var state = Services.GetRequiredService<SharedState>();
        Assert.Empty(state.Events);
    }

    [Fact]
    public async Task RunAsync_PipelineThrowsAggregateException_LogsAndDoesNotPropagate()
    {
        var group = new AggregateThrowingPipelineGroup();

        var context = new PipelineGroupExecutionContext()
        {
            ScopeFactory = Services.GetRequiredService<IServiceScopeFactory>(),
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        };

        var exception = await Record.ExceptionAsync(() => group.RunAsync(context));

        Assert.Null(exception);
    }

    [Fact]
    public async Task RunAsync_PipelineThrows_LogsAndDoesNotPropagate()
    {
        var group = new PlainThrowingPipelineGroup();

        var context = new PipelineGroupExecutionContext()
        {
            ScopeFactory = Services.GetRequiredService<IServiceScopeFactory>(),
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        };

        var exception = await Record.ExceptionAsync(() => group.RunAsync(context));

        Assert.Null(exception);
    }

    private class SharedState
    {
        public List<string> Events { get; } = new();

        public void Add(string value)
        {
            lock (Events)
            {
                Events.Add(value);
            }
        }
    }

    private class TestPipelineGroup : PipelineGroup
    {
        public TestPipelineGroup()
        {
            // Add parent first, then child and declare dependency
            AddPipeline<ParentPipeline>();
            AddPipeline<ChildPipeline>()
                .WaitFor<ParentPipeline>();
        }
    }

    private class ManyParentsPipelineGroup : PipelineGroup
    {
        public ManyParentsPipelineGroup()
        {
            AddPipeline<ParentPipelineA>();
            AddPipeline<ParentPipelineB>();
            AddPipeline<ParentPipelineC>();

            AddPipeline<ChildManyParentsPipeline>()
                .WaitFor<ParentPipelineA>()
                .WaitFor<ParentPipelineB>()
                .WaitFor<ParentPipelineC>();
        }
    }

    private class DuplicatePipelineGroup : PipelineGroup
    {
        public DuplicatePipelineGroup()
        {
            AddPipeline<ParentPipeline>();
            AddPipeline<ParentPipeline>();
        }
    }

    private class EmptyPipelineGroup : PipelineGroup;

    private class AggregateThrowingPipelineGroup : PipelineGroup
    {
        public AggregateThrowingPipelineGroup() => AddPipeline<AggregateThrowingPipeline>();
    }

    private class PlainThrowingPipelineGroup : PipelineGroup
    {
        public PlainThrowingPipelineGroup() => AddPipeline<PlainThrowingPipeline>();
    }

    // IPipeline.RunAsync itself is expected to swallow its own failures (see Pipeline<,>.RunAsync);
    // these two exist only to exercise PipelineGroup's defensive catch around Task.WhenAll(tasks).
    private class AggregateThrowingPipeline : IPipeline
    {
        public Task RunAsync(PipelineExecutionContext context) =>
            Task.FromException(new AggregateException("boom", new InvalidOperationException("inner")));

        public Task<PipelineOutcome> CompletionTask => Task.FromResult(PipelineOutcome.Failure);
    }

    private class PlainThrowingPipeline : IPipeline
    {
        public Task RunAsync(PipelineExecutionContext context) =>
            Task.FromException(new InvalidOperationException("boom"));

        public Task<PipelineOutcome> CompletionTask => Task.FromResult(PipelineOutcome.Failure);
    }

    // Awaits any parent pipelines (there are none for the "parent" roles below), then
    // records its name into SharedState. One base covers every fake pipeline this file needs;
    // each pipeline still needs its own concrete type since PipelineGroup keys dependencies by Type.
    private abstract class RecordingPipeline(SharedState state, string name, int delayMs = 20) : IPipeline
    {
        private readonly TaskCompletionSource<PipelineOutcome> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<PipelineOutcome> CompletionTask => _tcs.Task;

        public async Task RunAsync(PipelineExecutionContext context)
        {
            try
            {
                var parentTasks = context.ParentPipelines.Select(p => p.CompletionTask).ToArray();
                if (parentTasks.Length > 0)
                {
                    await Task.WhenAll(parentTasks);
                }

                await Task.Delay(delayMs, context.Token);
                state.Add(name);
                _tcs.TrySetResult(PipelineOutcome.Success);
            }
            catch (OperationCanceledException)
            {
                _tcs.TrySetResult(PipelineOutcome.Canceled);
                throw;
            }
            catch (Exception)
            {
                _tcs.TrySetResult(PipelineOutcome.Failure);
                throw;
            }
        }
    }

    private class ParentPipeline(SharedState state) : RecordingPipeline(state, "parent");
    private class ChildPipeline(SharedState state) : RecordingPipeline(state, "child", delayMs: 5);

    private class ParentPipelineA(SharedState state) : RecordingPipeline(state, nameof(ParentPipelineA));
    private class ParentPipelineB(SharedState state) : RecordingPipeline(state, nameof(ParentPipelineB));
    private class ParentPipelineC(SharedState state) : RecordingPipeline(state, nameof(ParentPipelineC));
    private class ChildManyParentsPipeline(SharedState state) : RecordingPipeline(state, "child-many", delayMs: 5);
}
