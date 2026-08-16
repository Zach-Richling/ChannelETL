namespace ChannelETL.Tests;

public class BatchedPipelineDestinationTests
{
    private class TestBatchedDestination : BatchedPipelineDestination<int>
    {
        public List<List<int>> ReceivedBatches { get; } = new();

        public TestBatchedDestination() { }
        public TestBatchedDestination(int batchSize) : base(batchSize) { }

        public override Task ConsumeBatchAsync(IEnumerable<int> batch, CancellationToken token)
        {
            ReceivedBatches.Add(batch.ToList());
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task ConsumeAsync_EmitsBatchWhenSizeReached()
    {
        var dest = new TestBatchedDestination();
        var pd = (IPipelineDestination<int>)dest;

        for (int i = 1; i <= 10; i++)
        {
            await pd.ConsumeAsync(i, CancellationToken.None);
        }

        // After 10 items (default batch size) one batch should have been emitted
        Assert.Single(dest.ReceivedBatches);
        var batch = dest.ReceivedBatches[0];
        Assert.Equal(10, batch.Count);
        Assert.Equal(Enumerable.Range(1, 10), batch);
    }

    [Fact]
    public async Task CompleteAsync_EmitsRemainingItems()
    {
        var dest = new TestBatchedDestination();
        var pd = (IPipelineDestination<int>)dest;

        // send fewer than batch size
        await pd.ConsumeAsync(1, CancellationToken.None);
        await pd.ConsumeAsync(2, CancellationToken.None);

        await pd.CompleteAsync(CancellationToken.None);

        Assert.Single(dest.ReceivedBatches);
        var batch = dest.ReceivedBatches[0];
        Assert.Equal(new[] { 1, 2 }, batch);
    }

    [Fact]
    public async Task CustomBatchSize_WorksAcrossMultipleEmitsAndComplete()
    {
        var dest = new TestBatchedDestination(batchSize: 3);
        var pd = (IPipelineDestination<int>)dest;

        // Produce 7 items -> two full batches (3,3) and one final batch of 1 after Complete
        for (int i = 1; i <= 7; i++)
            await pd.ConsumeAsync(i, CancellationToken.None);

        // two batches emitted so far
        Assert.Equal(2, dest.ReceivedBatches.Count);
        Assert.Equal(new[] { 1, 2, 3 }, dest.ReceivedBatches[0]);
        Assert.Equal(new[] { 4, 5, 6 }, dest.ReceivedBatches[1]);

        await pd.CompleteAsync(CancellationToken.None);

        Assert.Equal(3, dest.ReceivedBatches.Count);
        Assert.Equal(new[] { 7 }, dest.ReceivedBatches[2]);
    }
}
