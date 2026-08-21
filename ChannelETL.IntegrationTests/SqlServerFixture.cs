using Dapper;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace ChannelETL.IntegrationTests;

/// <summary>
/// Starts a single SQL Server container for the whole test run and seeds the schema the
/// DapperPipelineSource tests read from. The source under test is read-only, so every test
/// can safely share the same seeded data.
/// </summary>
public sealed class SqlServerFixture : IAsyncLifetime
{
    private const string SqlServerImage = "mcr.microsoft.com/mssql/server:2022-latest";

    private readonly MsSqlContainer _container = new MsSqlBuilder(SqlServerImage).Build();

    /// <summary>The rows seeded into dbo.Orders, ordered by Id.</summary>
    public static IReadOnlyList<Order> SeededOrders { get; } =
        [.. Enumerable.Range(1, 10).Select(i => new Order { Id = i, Name = $"order-{i}" })];

    public string ConnectionString => _container.GetConnectionString();

    public SqlConnection CreateConnection() => new(ConnectionString);

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        await using var connection = CreateConnection();

        await connection.ExecuteAsync(
            """
            CREATE TABLE dbo.Orders
            (
                Id INT NOT NULL PRIMARY KEY,
                Name NVARCHAR(100) NOT NULL
            );
            """);

        await connection.ExecuteAsync(
            "INSERT INTO dbo.Orders (Id, Name) VALUES (@Id, @Name);",
            SeededOrders);

        // CREATE PROCEDURE must be the first statement in its batch, so each one is sent separately.
        await connection.ExecuteAsync(
            """
            CREATE PROCEDURE dbo.usp_GetOrders
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT Id, Name FROM dbo.Orders ORDER BY Id;
            END
            """);

        await connection.ExecuteAsync(
            """
            CREATE PROCEDURE dbo.usp_GetOrdersAfter
                @MinId INT
            AS
            BEGIN
                SET NOCOUNT ON;
                SELECT Id, Name FROM dbo.Orders WHERE Id > @MinId ORDER BY Id;
            END
            """);
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

[CollectionDefinition(Name)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = nameof(SqlServerCollection);
}
