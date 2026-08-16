using Microsoft.Extensions.Logging;
using NSubstitute;

namespace ChannelETL.Tests;

public class PipelineTests
{
    [Fact]
    public async Task CompletesAndPreservesOrder()
    {
        var source = TestComponents.CreateTestSource(() => AsyncEnumerable.Range(1, 5));
        var transform = TestComponents.CreateTestTransform<int, string>(async (i, ct) => i.ToString());
        var destination = TestComponents.CreateTestDestination<string>();
        var logger = Substitute.For<ILogger<Pipeline<int, string>>>();

        var pipeline = new Pipeline<int, string>(logger)
        {
            Source = source,
            Transform = transform,
            Destination = destination,
            Name = nameof(CompletesAndPreservesOrder),
            ParentPipelines = []
        };

        await pipeline.RunAsync(CancellationToken.None);
        var outcome = await pipeline.CompletionTask;

        var consumed = destination.Items.ToList();
        var expected = Enumerable.Range(1, 5).Select(i => i.ToString()).ToList();
        Assert.Equal(expected, consumed);
        Assert.Equal(PipelineOutcome.Success, outcome);
    }

    [Fact]
    public async Task CancelledDuringRun_PartialConsumption()
    {
        var source = TestComponents.CreateTestSource(() => AsyncEnumerable.Range(1, 1000));
        var transform = TestComponents.CreateTestTransform<int, string>(async (i, ct) =>
        {
            await Task.Delay(20, ct);
            return i.ToString();
        });
        var destination = TestComponents.CreateTestDestination<string>();
        var logger = Substitute.For<ILogger<Pipeline<int, string>>>();

        var pipeline = new Pipeline<int, string>(logger)
        {
            Source = source,
            Transform = transform,
            Destination = destination,
            Name = nameof(CancelledDuringRun_PartialConsumption),
            ParentPipelines = []
        };

        using var cts = new CancellationTokenSource(100);

        await pipeline.RunAsync(cts.Token);
        var outcome = await pipeline.CompletionTask;

        var consumed = destination.Items.ToList();
        Assert.True(consumed.Count > 0 && consumed.Count < 1000, "Destination should have consumed a partial number of items before cancellation.");

        var asInts = consumed.Select(int.Parse).ToList();
        Assert.Equal(asInts.OrderBy(x => x), asInts);
        Assert.Equal(PipelineOutcome.Canceled, outcome);
    }

    [Fact]
    public async Task TransformThrows_DestinationReceivesPriorItems()
    {
        var source = TestComponents.CreateTestSource(() => AsyncEnumerable.Range(1, 5));
        var transform = TestComponents.CreateTestTransform<int, string>(async (i, ct) =>
        {
            if (i == 3)
                throw new InvalidOperationException("boom");

            await Task.Yield();
            return i.ToString();
        });
        var destination = TestComponents.CreateTestDestination<string>();
        var logger = Substitute.For<ILogger<Pipeline<int, string>>>();

        var pipeline = new Pipeline<int, string>(logger)
        {
            Source = source,
            Transform = transform,
            Destination = destination,
            Name = nameof(TransformThrows_DestinationReceivesPriorItems),
            ParentPipelines = []
        };

        await pipeline.RunAsync(CancellationToken.None);
        var outcome = await pipeline.CompletionTask;

        var consumed = destination.Items.ToList();
        var expectedPrior = new List<string> { "1", "2" };
        Assert.Equal(expectedPrior, consumed);
        Assert.Equal(PipelineOutcome.Failure, outcome);
    }
}
