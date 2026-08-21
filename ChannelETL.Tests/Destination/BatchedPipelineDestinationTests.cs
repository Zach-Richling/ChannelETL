using NSubstitute;

namespace ChannelETL.Tests;

public class BatchedPipelineDestinationTests
{
    [Fact]
    public async Task ConsumeAsync_EmitsBatchWhenSizeReached()
    {
        var (destination, batches) = CreateDestination();
        var pd = (IPipelineDestination<int>)destination;

        for (int i = 1; i <= 10; i++)
        {
            await pd.ConsumeAsync(i, CancellationToken.None);
        }

        // After 10 items (default batch size) one batch should have been emitted
        var batch = Assert.Single(batches);
        Assert.Equal(10, batch.Count);
        Assert.Equal(Enumerable.Range(1, 10), batch);
    }

    [Fact]
    public async Task CompleteAsync_EmitsRemainingItems()
    {
        var (destination, batches) = CreateDestination();
        var pd = (IPipelineDestination<int>)destination;

        // send fewer than batch size
        await pd.ConsumeAsync(1, CancellationToken.None);
        await pd.ConsumeAsync(2, CancellationToken.None);

        await pd.CompleteAsync(CancellationToken.None);

        var batch = Assert.Single(batches);
        Assert.Equal(new[] { 1, 2 }, batch);
    }

    [Fact]
    public async Task CustomBatchSize_WorksAcrossMultipleEmitsAndComplete()
    {
        var (destination, batches) = CreateDestination(batchSize: 3);
        var pd = (IPipelineDestination<int>)destination;

        // Produce 7 items -> two full batches (3,3) and one final batch of 1 after Complete
        for (int i = 1; i <= 7; i++)
            await pd.ConsumeAsync(i, CancellationToken.None);

        // two batches emitted so far
        Assert.Equal(2, batches.Count);
        Assert.Equal(new[] { 1, 2, 3 }, batches[0]);
        Assert.Equal(new[] { 4, 5, 6 }, batches[1]);

        await pd.CompleteAsync(CancellationToken.None);

        Assert.Equal(3, batches.Count);
        Assert.Equal(new[] { 7 }, batches[2]);
    }

    // Substitute.ForPartsOf lets the real batching logic in BatchedPipelineDestination run
    // while only the abstract ConsumeBatchAsync member is a substitute we can observe.
    private static (BatchedPipelineDestination<int> Destination, List<List<int>> Batches) CreateDestination(int? batchSize = null)
    {
        var destination = batchSize is int size
            ? Substitute.ForPartsOf<BatchedPipelineDestination<int>>(size)
            : Substitute.ForPartsOf<BatchedPipelineDestination<int>>();

        var batches = new List<List<int>>();
        destination.When(x => x.ConsumeBatchAsync(Arg.Any<IReadOnlyList<int>>(), Arg.Any<CancellationToken>()))
            .Do(ci => batches.Add([.. ci.Arg<IReadOnlyList<int>>()]));

        return (destination, batches);
    }
}
