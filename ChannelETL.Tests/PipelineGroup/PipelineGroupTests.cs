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
