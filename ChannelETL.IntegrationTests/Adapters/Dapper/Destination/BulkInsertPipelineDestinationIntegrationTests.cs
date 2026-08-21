using ChannelETL.Adapters.Dapper;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using System.Data;
using System.Data.Common;

namespace ChannelETL.IntegrationTests;

/// <summary>
/// Exercises BulkInsertPipelineDestination against a real SQL Server instance. SqlBulkCopy is
/// sealed and takes a sealed SqlConnection, so the write path has no seam for a test double - the
/// column mapping, the per-batch transaction and cancellation only really run here.
/// Each test writes to its own freshly created table so the shared dbo.Orders seed stays intact.
/// </summary>
[Collection(SqlServerCollection.Name)]
public class BulkInsertPipelineDestinationIntegrationTests(SqlServerFixture fixture)
{
    [Fact]
    public async Task CompleteAsync_FewerItemsThanBatchSize_WritesEveryRow()
    {
        var table = await CreateOrderTableAsync();
        await using var connection = fixture.CreateConnection();
        var destination = (IPipelineDestination<Order>)new TestBulkInsertDestination<Order>(connection, table, batchSize: 100);

        var orders = CreateOrders(1, 5);
        await ConsumeAllAsync(destination, orders);

        // Nothing has hit the batch size, so the rows only land when CompleteAsync flushes.
        Assert.Empty(await ReadOrdersAsync(table));

        await destination.CompleteAsync(CancellationToken.None);

        Assert.Equal(orders, await ReadOrdersAsync(table));
    }

    [Fact]
    public async Task ConsumeAsync_ReachingBatchSize_WritesBeforeCompleteAsync()
    {
        var table = await CreateOrderTableAsync();
        await using var connection = fixture.CreateConnection();
        var destination = (IPipelineDestination<Order>)new TestBulkInsertDestination<Order>(connection, table, batchSize: 3);

        var orders = CreateOrders(1, 3);
        await ConsumeAllAsync(destination, orders);

        // UseInternalTransaction commits each batch on its own, so a full batch is durable
        // without waiting for the destination to complete.
        Assert.Equal(orders, await ReadOrdersAsync(table));
    }

    [Fact]
    public async Task ConsumeAsync_ManyBatches_WritesEveryBatchExactlyOnce()
    {
        var table = await CreateOrderTableAsync();
        await using var connection = fixture.CreateConnection();
        var destination = (IPipelineDestination<Order>)new TestBulkInsertDestination<Order>(connection, table, batchSize: 3);

        // 7 items over a batch size of 3: two full batches plus a remainder at completion,
        // each one a separate SqlBulkCopy over the same connection.
        var orders = CreateOrders(1, 7);
        await ConsumeAllAsync(destination, orders);
        await destination.CompleteAsync(CancellationToken.None);

        Assert.Equal(orders, await ReadOrdersAsync(table));
    }

    [Fact]
    public async Task ConsumeBatchAsync_ClosedConnection_OpensItAndWrites()
    {
        var table = await CreateOrderTableAsync();

        // SqlBulkCopy never opens the connection it is handed, so the destination has to - a
        // connection resolved straight from DI arrives closed.
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<Order>(connection, table, batchSize: 2);

        await destination.ConsumeBatchAsync(CreateOrders(1, 2), CancellationToken.None);

        Assert.Equal(CreateOrders(1, 2), await ReadOrdersAsync(table));
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task ConsumeBatchAsync_AlreadyOpenConnection_WritesAndLeavesItUsable()
    {
        var table = await CreateOrderTableAsync();
        await using var connection = await OpenConnectionAsync();
        var destination = new TestBulkInsertDestination<Order>(connection, table, batchSize: 2);

        // A caller that opened the connection itself must not trip the open-if-closed guard,
        // and neither batch may leave the connection unusable for the next one.
        await destination.ConsumeBatchAsync(CreateOrders(1, 2), CancellationToken.None);
        await destination.ConsumeBatchAsync(CreateOrders(3, 2), CancellationToken.None);

        Assert.Equal(CreateOrders(1, 4), await ReadOrdersAsync(table));

        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Equal(4, await connection.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {table};"));
    }

    [Fact]
    public async Task ConsumeBatchAsync_NullProperties_WriteAsNulls()
    {
        var table = await CreateTableAsync("Id INT NOT NULL PRIMARY KEY, Name NVARCHAR(100) NULL, Amount DECIMAL(18,2) NULL");
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<NullableRow>(connection, table, batchSize: 2);

        var rows = new List<NullableRow>
        {
            new() { Id = 1, Name = "first", Amount = 12.50m },
            new() { Id = 2, Name = null, Amount = null }
        };

        await destination.ConsumeBatchAsync(rows, CancellationToken.None);

        await using var reader = fixture.CreateConnection();
        var written = (await reader.QueryAsync<NullableRow>($"SELECT Id, Name, Amount FROM {table} ORDER BY Id;")).ToList();

        Assert.Equal(rows, written);
    }

    [Fact]
    public async Task ConsumeBatchAsync_DestinationColumnOrderDiffersFromProperties_MapsByName()
    {
        // Columns deliberately declared in the opposite order to the properties on Order: the
        // destination maps by name, so the values must not be written positionally.
        var table = await CreateTableAsync("Name NVARCHAR(100) NOT NULL, Id INT NOT NULL PRIMARY KEY");
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<Order>(connection, table, batchSize: 3);

        var orders = CreateOrders(1, 3);
        await destination.ConsumeBatchAsync(orders, CancellationToken.None);

        Assert.Equal(orders, await ReadOrdersAsync(table));
    }

    [Fact]
    public async Task ConsumeBatchAsync_ExtraColumnsInDestinationTable_AreLeftAtTheirDefault()
    {
        var table = await CreateTableAsync(
            "Id INT NOT NULL PRIMARY KEY, Name NVARCHAR(100) NOT NULL, Note NVARCHAR(50) NOT NULL DEFAULT 'untouched'");
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<Order>(connection, table, batchSize: 2);

        await destination.ConsumeBatchAsync(CreateOrders(1, 2), CancellationToken.None);

        await using var reader = fixture.CreateConnection();
        var notes = await reader.QueryAsync<string>($"SELECT Note FROM {table};");

        Assert.All(notes, note => Assert.Equal("untouched", note));
    }

    [Fact]
    public async Task ConsumeBatchAsync_RowViolatesConstraint_RollsBackTheWholeBatch()
    {
        var table = await CreateOrderTableAsync();
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<Order>(connection, table, batchSize: 3);

        var orders = new List<Order>
        {
            new() { Id = 1, Name = "first" },
            new() { Id = 2, Name = "second" },
            new() { Id = 1, Name = "duplicate" }
        };

        await Assert.ThrowsAsync<SqlException>(() => destination.ConsumeBatchAsync(orders, CancellationToken.None));

        // SqlBulkCopyOptions.UseInternalTransaction wraps the batch, so the two good rows that
        // preceded the primary key violation must not survive either.
        Assert.Empty(await ReadOrdersAsync(table));
    }

    [Fact]
    public async Task ConsumeBatchAsync_CanceledToken_WritesNothing()
    {
        var table = await CreateOrderTableAsync();
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<Order>(connection, table, batchSize: 3);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => destination.ConsumeBatchAsync(CreateOrders(1, 3), cts.Token));

        Assert.Empty(await ReadOrdersAsync(table));
    }

    [Fact]
    public async Task ConsumeBatchAsync_UnknownDestinationTable_Throws()
    {
        await using var connection = fixture.CreateConnection();
        var destination = new TestBulkInsertDestination<Order>(connection, "dbo.NoSuchTable", batchSize: 2);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => destination.ConsumeBatchAsync(CreateOrders(1, 2), CancellationToken.None));
    }

    [Fact]
    public async Task Pipeline_FromDapperSourceIntoBulkInsert_CopiesEveryRow()
    {
        var table = await CreateOrderTableAsync();
        await using var sourceConnection = fixture.CreateConnection();
        await using var destinationConnection = fixture.CreateConnection();

        var pipeline = new OrderCopyPipeline(
            new OrderSource(sourceConnection),
            new IdentityTransformation<Order>(),
            new TestBulkInsertDestination<Order>(destinationConnection, table, batchSize: 3));

        await pipeline.RunAsync(new PipelineExecutionContext
        {
            ParentPipelines = [],
            Logger = NullLogger.Instance,
            Token = CancellationToken.None
        });

        Assert.Equal(PipelineOutcome.Success, await pipeline.CompletionTask);
        Assert.Equal(SqlServerFixture.SeededOrders, await ReadOrdersAsync(table));
    }

    private Task<string> CreateOrderTableAsync()
        => CreateTableAsync("Id INT NOT NULL PRIMARY KEY, Name NVARCHAR(100) NOT NULL");

    /// <summary>Creates a uniquely named table so tests never write over each other.</summary>
    private async Task<string> CreateTableAsync(string columnDefinitions)
    {
        var tableName = $"dbo.BulkTarget_{Guid.NewGuid():N}";

        await using var connection = fixture.CreateConnection();
        await connection.ExecuteAsync($"CREATE TABLE {tableName} ({columnDefinitions});");

        return tableName;
    }

    /// <summary>An already-open connection, for the tests that check the destination copes with one.</summary>
    private async Task<SqlConnection> OpenConnectionAsync()
    {
        var connection = fixture.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        return connection;
    }

    private async Task<List<Order>> ReadOrdersAsync(string tableName)
    {
        await using var connection = fixture.CreateConnection();
        return [.. await connection.QueryAsync<Order>($"SELECT Id, Name FROM {tableName} ORDER BY Id;")];
    }

    private static List<Order> CreateOrders(int firstId, int count)
        => [.. Enumerable.Range(firstId, count).Select(i => new Order { Id = i, Name = $"order-{i}" })];

    private static async Task ConsumeAllAsync(IPipelineDestination<Order> destination, IEnumerable<Order> orders)
    {
        foreach (var order in orders)
        {
            await destination.ConsumeAsync(order, CancellationToken.None);
        }
    }

    /// <summary>Row shape with nullable columns, to check how nulls reach the server.</summary>
    public sealed record NullableRow
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public decimal? Amount { get; set; }
    }

    // BulkInsertPipelineDestination is abstract, so tests need a concrete derived type.
    private sealed class TestBulkInsertDestination<T>(SqlConnection connection, string tableName, int batchSize)
        : BulkInsertPipelineDestination<T>(connection, tableName, batchSize);

    private sealed class OrderSource : DapperPipelineSource<Order>
    {
        public OrderSource(DbConnection connection) : base(connection)
            => Text = "SELECT Id, Name FROM dbo.Orders ORDER BY Id;";
    }

    private sealed class IdentityTransformation<T> : IPipelineTransformation<T, T>
    {
        public Task<T> TransformAsync(T item, CancellationToken token) => Task.FromResult(item);
    }

    private sealed class OrderCopyPipeline(
        IPipelineSource<Order> source,
        IPipelineTransformation<Order, Order> transform,
        IPipelineDestination<Order> destination)
        : Pipeline<Order, Order>(source, transform, destination);
}
