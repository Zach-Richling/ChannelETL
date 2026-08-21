using ChannelETL.Adapters.Dapper;
using Microsoft.Data.SqlClient;
using NSubstitute;
using System.Data;

namespace ChannelETL.Tests;

/// <summary>
/// Covers what BulkInsertPipelineDestination adds on top of BatchedPipelineDestination without a
/// server: the batch size it hands to the base class, and the fact that nothing reaches the
/// connection until a batch is actually flushed. The write itself runs through SqlBulkCopy, which
/// is sealed and takes a sealed SqlConnection, so it has no seam for a test double and is covered
/// by the integration tests instead.
/// </summary>
public class BulkInsertPipelineDestinationTests
{
    // Never dialed: a SqlConnection only contacts the server when it is opened.
    private const string UnopenedConnectionString = "Server=(local);Database=Unused;Integrated Security=true;";

    [Fact]
    public async Task ConsumeAsync_ReachingConfiguredBatchSize_FlushesThatManyItems()
    {
        var (destination, batches) = CreateInterceptedDestination(batchSize: 3);
        var pd = (IPipelineDestination<int>)destination;

        for (int i = 1; i <= 3; i++)
        {
            await pd.ConsumeAsync(i, CancellationToken.None);
        }

        // The batch size travels through the primary constructor into the base class, so it is
        // the third item - not the base class default of ten - that triggers the flush.
        var batch = Assert.Single(batches);
        Assert.Equal([1, 2, 3], batch);
    }

    [Fact]
    public async Task ConsumeAsync_MoreItemsThanBatchSize_FlushesEachBatchInOrder()
    {
        var (destination, batches) = CreateInterceptedDestination(batchSize: 2);
        var pd = (IPipelineDestination<int>)destination;

        for (int i = 1; i <= 5; i++)
        {
            await pd.ConsumeAsync(i, CancellationToken.None);
        }

        await pd.CompleteAsync(CancellationToken.None);

        Assert.Equal(3, batches.Count);
        Assert.Equal([1, 2], batches[0]);
        Assert.Equal([3, 4], batches[1]);
        Assert.Equal([5], batches[2]);
    }

    [Fact]
    public async Task ConsumeAsync_BelowBatchSize_LeavesTheConnectionUntouched()
    {
        using var connection = new SqlConnection(UnopenedConnectionString);
        var destination = new TestBulkInsertDestination<int>(connection, "dbo.Target", batchSize: 3);
        var pd = (IPipelineDestination<int>)destination;

        await pd.ConsumeAsync(1, CancellationToken.None);
        await pd.ConsumeAsync(2, CancellationToken.None);

        // A flush would have had to reach the (unreachable) server, so a still-closed connection
        // is what proves buffering happened instead.
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public async Task CompleteAsync_WithoutAnyItems_DoesNotAttemptAnEmptyBulkCopy()
    {
        using var connection = new SqlConnection(UnopenedConnectionString);
        var destination = new TestBulkInsertDestination<int>(connection, "dbo.Target", batchSize: 3);

        await destination.CompleteAsync(CancellationToken.None);

        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public async Task ConsumeAsync_CanceledToken_ThrowsBeforeBuffering()
    {
        var (destination, batches) = CreateInterceptedDestination(batchSize: 1);
        var pd = (IPipelineDestination<int>)destination;

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pd.ConsumeAsync(1, cts.Token));

        Assert.Empty(batches);
    }

    // ForPartsOf runs the real batching logic while DoNotCallBase keeps the SqlBulkCopy write from
    // executing, which is the only way to observe the batches this class would have written.
    private static (BulkInsertPipelineDestination<int> Destination, List<List<int>> Batches) CreateInterceptedDestination(int batchSize)
    {
        var connection = new SqlConnection(UnopenedConnectionString);
        var destination = Substitute.ForPartsOf<BulkInsertPipelineDestination<int>>(connection, "dbo.Target", batchSize);

        var batches = new List<List<int>>();
        destination.When(x => x.ConsumeBatchAsync(Arg.Any<IReadOnlyList<int>>(), Arg.Any<CancellationToken>()))
            .DoNotCallBase();
        destination.When(x => x.ConsumeBatchAsync(Arg.Any<IReadOnlyList<int>>(), Arg.Any<CancellationToken>()))
            .Do(ci => batches.Add([.. ci.Arg<IReadOnlyList<int>>()]));

        return (destination, batches);
    }

    // BulkInsertPipelineDestination is abstract, so tests that want the real ConsumeBatchAsync
    // need a concrete derived type.
    private sealed class TestBulkInsertDestination<T>(SqlConnection connection, string tableName, int batchSize)
        : BulkInsertPipelineDestination<T>(connection, tableName, batchSize);
}
