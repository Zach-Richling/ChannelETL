using ChannelETL.Adapters.Dapper;
using NSubstitute;
using System.Data;
using System.Data.Common;

namespace ChannelETL.Tests;

public class DapperPipelineSourceTests
{
    [Fact]
    public void Sql_TextCommandType_ReturnsText()
    {
        var source = CreateSource(CommandType.Text, text: "SELECT Id FROM Orders");

        Assert.Equal("SELECT Id FROM Orders", source.ResolvedSql);
    }

    [Fact]
    public void Sql_StoredProcedureCommandType_ReturnsStoredProcedureName()
    {
        var source = CreateSource(CommandType.StoredProcedure, storedProcedureName: "usp_GetOrders");

        Assert.Equal("usp_GetOrders", source.ResolvedSql);
    }

    [Fact]
    public void Sql_TableDirectCommandType_ReturnsTableName()
    {
        var source = CreateSource(CommandType.TableDirect, tableName: "Orders");

        Assert.Equal("Orders", source.ResolvedSql);
    }

    [Theory]
    [InlineData(CommandType.Text, "Text")]
    [InlineData(CommandType.StoredProcedure, "StoredProcedureName")]
    [InlineData(CommandType.TableDirect, "TableName")]
    public void Sql_MissingConfigurationForCommandType_ThrowsArgumentException(CommandType commandType, string expectedParamName)
    {
        var source = CreateSource(commandType);

        var exception = Assert.Throws<ArgumentException>(() => _ = source.ResolvedSql);

        Assert.Equal(expectedParamName, exception.ParamName);
    }

    [Fact]
    public void Sql_UnsupportedCommandType_ThrowsNotSupportedException()
    {
        var source = CreateSource((CommandType)999);

        Assert.Throws<NotSupportedException>(() => _ = source.ResolvedSql);
    }

    [Fact]
    public async Task ProduceAsync_NotEnumerated_DoesNotEvaluateSql()
    {
        var source = CreateSource(CommandType.Text);

        // ProduceAsync is an async iterator, so its body - including the Sql evaluation
        // that is about to throw - must not run until the first MoveNextAsync
        var enumerable = source.ProduceAsync(CancellationToken.None);

        await Assert.ThrowsAsync<ArgumentException>(() => DrainAsync(enumerable));
    }

    [Fact]
    public async Task ProduceAsync_InvalidConfiguration_NeverOpensConnection()
    {
        var connection = Substitute.For<DbConnection>();
        var source = new TestDapperSource<int>(connection, CommandType.Text);

        await Assert.ThrowsAsync<ArgumentException>(() => DrainAsync(source.ProduceAsync(CancellationToken.None)));

        connection.DidNotReceive().Open();
        await connection.DidNotReceive().OpenAsync(Arg.Any<CancellationToken>());
    }

    // The connection is irrelevant to Sql resolution - it is never touched.
    private static TestDapperSource<int> CreateSource(
        CommandType commandType,
        string? text = null,
        string? tableName = null,
        string? storedProcedureName = null)
        => new(Substitute.For<DbConnection>(), commandType, text, tableName, storedProcedureName);

    private static async Task DrainAsync<T>(IAsyncEnumerable<T> source)
    {
        await foreach (var _ in source) { }
    }

    // DapperPipelineSource is abstract and its members are protected, so a derived type is
    // needed both to assign the init-only config properties and to expose Sql to the tests.
    private sealed class TestDapperSource<T> : DapperPipelineSource<T>
    {
        public TestDapperSource(
            DbConnection connection,
            CommandType commandType = CommandType.Text,
            string? text = null,
            string? tableName = null,
            string? storedProcedureName = null,
            object? parameters = null)
            : base(connection)
        {
            CommandType = commandType;
            Text = text;
            TableName = tableName;
            StoredProcedureName = storedProcedureName;
            Parameters = parameters;
        }

        public string ResolvedSql => Sql;
    }
}
