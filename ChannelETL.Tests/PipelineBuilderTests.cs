namespace ChannelETL.Tests;

public class PipelineBuilderTests
{
    [Fact]
    public void Build_WithAllComponents_ReturnsPipelineWithThoseComponents()
    {
        var source = TestComponents.CreateTestSource(() => AsyncEnumerable.Range(1, 3));
        var transform = TestComponents.CreateTestTransform<int, string>(async (i, ct) => i.ToString());
        var destination = TestComponents.CreateTestDestination<string>();

        var builder = new PipelineBuilder<int, string>()
            .WithSource(source)
            .WithTransformation(transform)
            .WithDestination(destination);

        var pipeline = builder.Build();

        Assert.Same(source, pipeline.Source);
        Assert.Same(transform, pipeline.Transform);
        Assert.Same(destination, pipeline.Destination);
    }

    [Fact]
    public void Build_WithoutSource_ThrowsArgumentNullException()
    {
        var transform = TestComponents.CreateTestTransform<int, string>(async (i, ct) => i.ToString());
        var destination = TestComponents.CreateTestDestination<string>();

        var builder = new PipelineBuilder<int, string>()
            .WithTransformation(transform)
            .WithDestination(destination);

        Assert.Throws<ArgumentNullException>(builder.Build);
    }

    [Fact]
    public void Build_WithoutTransformation_ThrowsArgumentNullException()
    {
        var source = TestComponents.CreateTestSource(() => AsyncEnumerable.Range(1, 3));
        var destination = TestComponents.CreateTestDestination<string>();

        var builder = new PipelineBuilder<int, string>()
            .WithSource(source)
            .WithDestination(destination);

        Assert.Throws<ArgumentNullException>(builder.Build);
    }

    [Fact]
    public void Build_WithoutDestination_ThrowsArgumentNullException()
    {
        var source = TestComponents.CreateTestSource(() => AsyncEnumerable.Range(1, 3));
        var transform = TestComponents.CreateTestTransform<int, string>(async (i, ct) => i.ToString());

        var builder = new PipelineBuilder<int, string>()
            .WithSource(source)
            .WithTransformation(transform);

        Assert.Throws<ArgumentNullException>(builder.Build);
    }
}
