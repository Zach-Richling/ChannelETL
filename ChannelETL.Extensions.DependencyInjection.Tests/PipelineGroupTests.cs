using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChannelETL.Extensions.DependencyInjection.Tests;

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

        // Act
        await group.RunAsync(context);

        // Assert
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

        // Act
        await group.RunAsync(context);

        // Assert
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

    private class ParentPipeline(SharedState state) : IPipeline
    {
        private readonly TaskCompletionSource<PipelineOutcome> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<PipelineOutcome> CompletionTask => _tcs.Task;

        public async Task RunAsync(PipelineExecutionContext context)
        {
            try
            {
                // Simulate some work
                await Task.Delay(20, context.Token);
                state.Add("parent");
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

    private class ChildPipeline(SharedState state) : IPipeline
    {
        private readonly TaskCompletionSource<PipelineOutcome> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<PipelineOutcome> CompletionTask => _tcs.Task;

        public async Task RunAsync(PipelineExecutionContext context)
        {
            try
            {
                // Respect parent dependencies by awaiting their completion tasks
                var parentTasks = context.ParentPipelines.Select(p => p.CompletionTask);
                if (parentTasks.Any())
                {
                    await Task.WhenAll(parentTasks);
                }

                // Now perform child work
                await Task.Delay(5, context.Token);
                state.Add("child");
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

    private abstract class BaseParent(SharedState state, string name, int delayMs = 20) : IPipeline
    {
        protected readonly TaskCompletionSource<PipelineOutcome> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<PipelineOutcome> CompletionTask => _tcs.Task;

        public async Task RunAsync(PipelineExecutionContext context)
        {
            try
            {
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

    private class ParentPipelineA(SharedState state) : BaseParent(state, nameof(ParentPipelineA));
    private class ParentPipelineB(SharedState state) : BaseParent(state, nameof(ParentPipelineB));
    private class ParentPipelineC(SharedState state) : BaseParent(state, nameof(ParentPipelineC));

    private class ChildManyParentsPipeline(SharedState state) : IPipeline
    {
        private readonly TaskCompletionSource<PipelineOutcome> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<PipelineOutcome> CompletionTask => _tcs.Task;

        public async Task RunAsync(PipelineExecutionContext context)
        {
            try
            {
                // Wait for all parents to complete
                var parentTasks = context.ParentPipelines.Select(p => p.CompletionTask).ToArray();
                if (parentTasks.Length > 0)
                {
                    await Task.WhenAll(parentTasks);
                }

                // Child work
                await Task.Delay(5, context.Token);
                state.Add("child-many");
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
}