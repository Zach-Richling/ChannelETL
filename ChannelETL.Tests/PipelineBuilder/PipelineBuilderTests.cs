namespace ChannelETL.Tests;

public class PipelineBuilderTests
{
    [Fact]
    public void WaitFor_ParentInGroup_AddsParentAndReturnsBuilder()
    {
        var builder = new PipelineBuilder([typeof(PipelineA), typeof(PipelineB)]);

        var result = builder.WaitFor<PipelineA>();

        Assert.Same(builder, result);
        Assert.Equal(typeof(PipelineA), Assert.Single(builder.ParentPipelines));
    }

    [Fact]
    public void WaitFor_MultipleParentsInGroup_AddsAllParents()
    {
        var builder = new PipelineBuilder([typeof(PipelineA), typeof(PipelineB)]);

        builder.WaitFor<PipelineA>().WaitFor<PipelineB>();

        Assert.Equal(new HashSet<Type> { typeof(PipelineA), typeof(PipelineB) }, builder.ParentPipelines.ToHashSet());
    }

    [Fact]
    public void WaitFor_ParentNotInGroup_Throws()
    {
        var builder = new PipelineBuilder([typeof(PipelineA)]);

        Assert.Throws<InvalidOperationException>(() => builder.WaitFor<PipelineNotInGroup>());
    }

    [Fact]
    public void WaitFor_SameParentTwice_Throws()
    {
        var builder = new PipelineBuilder([typeof(PipelineA)]);
        builder.WaitFor<PipelineA>();

        Assert.Throws<InvalidOperationException>(() => builder.WaitFor<PipelineA>());
    }

    private class PipelineA : IPipeline
    {
        public Task RunAsync(PipelineExecutionContext context) => Task.CompletedTask;
        public Task<PipelineOutcome> CompletionTask => Task.FromResult(PipelineOutcome.Success);
    }

    private class PipelineB : IPipeline
    {
        public Task RunAsync(PipelineExecutionContext context) => Task.CompletedTask;
        public Task<PipelineOutcome> CompletionTask => Task.FromResult(PipelineOutcome.Success);
    }

    private class PipelineNotInGroup : IPipeline
    {
        public Task RunAsync(PipelineExecutionContext context) => Task.CompletedTask;
        public Task<PipelineOutcome> CompletionTask => Task.FromResult(PipelineOutcome.Success);
    }
}
