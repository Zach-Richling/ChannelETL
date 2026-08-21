using ChannelETL.Adapters.Dapper;
using Dapper;
using System.Data;
using System.Data.Common;

namespace ChannelETL.IntegrationTests;

/// <summary>
/// Exercises DapperPipelineSource against a real SQL Server instance. These cover the streaming
/// loop in ProduceAsync, which cannot run under a unit test because QueryUnbufferedAsync is a
/// static extension method with no seam for a test double.
/// </summary>
[Collection(SqlServerCollection.Name)]
public class DapperPipelineSourceIntegrationTests(SqlServerFixture fixture)
{
    [Fact]
    public async Task ProduceAsync_TextCommand_StreamsAllRowsInOrder()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders ORDER BY Id;");

        var produced = await CollectAsync(source.ProduceAsync(CancellationToken.None));

        Assert.Equal(SqlServerFixture.SeededOrders, produced);
    }

    [Fact]
    public async Task ProduceAsync_TextCommandWithParameters_AppliesParameters()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders WHERE Id > @MinId ORDER BY Id;",
            parameters: new { MinId = 7 });

        var produced = await CollectAsync(source.ProduceAsync(CancellationToken.None));

        Assert.Equal([8, 9, 10], produced.Select(o => o.Id));
    }

    [Fact]
    public async Task ProduceAsync_TextCommandMatchingNoRows_YieldsNothing()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders WHERE Id < 0;");

        var produced = await CollectAsync(source.ProduceAsync(CancellationToken.None));

        Assert.Empty(produced);
    }

    [Fact]
    public async Task ProduceAsync_StoredProcedure_StreamsAllRows()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.StoredProcedure,
            storedProcedureName: "dbo.usp_GetOrders");

        var produced = await CollectAsync(source.ProduceAsync(CancellationToken.None));

        Assert.Equal(SqlServerFixture.SeededOrders, produced);
    }

    [Fact]
    public async Task ProduceAsync_StoredProcedureWithParameters_AppliesParameters()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.StoredProcedure,
            storedProcedureName: "dbo.usp_GetOrdersAfter",
            parameters: new { MinId = 8 });

        var produced = await CollectAsync(source.ProduceAsync(CancellationToken.None));

        Assert.Equal([9, 10], produced.Select(o => o.Id));
    }

    [Fact]
    public async Task ProduceAsync_TokenCanceledBeforeEnumeration_ThrowsBeforeYieldingAnything()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders ORDER BY Id;");

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var produced = new List<Order>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var order in source.ProduceAsync(cts.Token))
            {
                produced.Add(order);
            }
        });

        Assert.Empty(produced);
    }

    [Fact]
    public async Task ProduceAsync_TokenCanceledMidEnumeration_StopsAfterCurrentItem()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders ORDER BY Id;");

        using var cts = new CancellationTokenSource();
        var produced = new List<Order>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var order in source.ProduceAsync(cts.Token))
            {
                produced.Add(order);

                if (produced.Count == 3)
                {
                    await cts.CancelAsync();
                }
            }
        });

        Assert.Equal([1, 2, 3], produced.Select(o => o.Id));
    }

    [Fact]
    public async Task ProduceAsync_CancellationSuppliedViaWithCancellation_IsHonored()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders ORDER BY Id;");

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // ProduceAsync's token parameter carries [EnumeratorCancellation], so a token supplied
        // through WithCancellation reaches it even though the call itself passed None.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in source.ProduceAsync(CancellationToken.None).WithCancellation(cts.Token))
            {
            }
        });
    }

    [Fact]
    public async Task ProduceAsync_EmptyResultSetWithCanceledToken_CompletesWithThrowing()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders WHERE Id < 0;");

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CollectAsync(source.ProduceAsync(cts.Token)));
    }

    [Fact]
    public async Task ProduceAsync_ConsumerStopsEarly_ReleasesTheReader()
    {
        await using var connection = fixture.CreateConnection();
        var source = new TestDapperSource<Order>(
            connection,
            CommandType.Text,
            text: "SELECT Id, Name FROM dbo.Orders ORDER BY Id;");

        var produced = new List<Order>();

        await foreach (var order in source.ProduceAsync(CancellationToken.None))
        {
            produced.Add(order);

            if (produced.Count == 2)
            {
                break;
            }
        }

        Assert.Equal([1, 2], produced.Select(o => o.Id));

        // Abandoning enumeration must dispose the underlying reader, otherwise the connection
        // would still be busy streaming and this second query would fail.
        var remaining = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.Orders;");
        Assert.Equal(SqlServerFixture.SeededOrders.Count, remaining);
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var items = new List<T>();

        await foreach (var item in source)
        {
            items.Add(item);
        }

        return items;
    }

    // DapperPipelineSource is abstract with protected init-only config, so a derived type is
    // needed to assign it. Mirrors the fixture in the unit test project.
    private sealed class TestDapperSource<T> : DapperPipelineSource<T>
    {
        public TestDapperSource(
            DbConnection connection,
            CommandType commandType = CommandType.Text,
            string? text = null,
            string? storedProcedureName = null,
            object? parameters = null)
            : base(connection)
        {
            CommandType = commandType;
            Text = text;
            StoredProcedureName = storedProcedureName;
            Parameters = parameters;
        }
    }
}
