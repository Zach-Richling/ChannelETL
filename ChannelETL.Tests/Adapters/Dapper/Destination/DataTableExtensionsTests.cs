using System.Data;

namespace ChannelETL.Tests;

/// <summary>
/// Covers the DataTable projection that BulkInsertPipelineDestination hands to SqlBulkCopy.
/// The shape it produces decides which columns get mapped, so the column set, the column types
/// and the null handling are all part of the destination contract.
/// </summary>
public class DataTableExtensionsTests
{
    [Fact]
    public void ToDataTable_NamesTheTableAfterTheItemType()
    {
        var table = new List<Row>().ToDataTable();

        Assert.Equal(nameof(Row), table.TableName);
    }

    [Fact]
    public void ToDataTable_CreatesOneColumnPerPublicInstanceProperty()
    {
        var table = new List<Row>().ToDataTable();

        Assert.Equal(["Id", "Name", "Amount", "CreatedOn"], table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact]
    public void ToDataTable_UnwrapsNullableColumnTypes()
    {
        var table = new List<Row>().ToDataTable();

        // SqlBulkCopy needs the underlying type - a column typed Nullable<T> would be rejected.
        Assert.Equal(typeof(int), table.Columns["Id"]!.DataType);
        Assert.Equal(typeof(string), table.Columns["Name"]!.DataType);
        Assert.Equal(typeof(decimal), table.Columns["Amount"]!.DataType);
        Assert.Equal(typeof(DateTime), table.Columns["CreatedOn"]!.DataType);
    }

    [Fact]
    public void ToDataTable_CopiesValuesInEnumerationOrder()
    {
        var createdOn = new DateTime(2026, 8, 21, 9, 30, 0, DateTimeKind.Utc);
        var rows = new List<Row>
        {
            new() { Id = 2, Name = "second", Amount = 20.5m, CreatedOn = createdOn },
            new() { Id = 1, Name = "first", Amount = 10.25m, CreatedOn = createdOn.AddDays(-1) }
        };

        var table = rows.ToDataTable();

        Assert.Equal(2, table.Rows.Count);
        Assert.Equal([2, "second", 20.5m, createdOn], table.Rows[0].ItemArray);
        Assert.Equal([1, "first", 10.25m, createdOn.AddDays(-1)], table.Rows[1].ItemArray);
    }

    [Fact]
    public void ToDataTable_NullPropertyValues_BecomeDbNull()
    {
        var rows = new List<Row> { new() { Id = 1, Name = null, Amount = null, CreatedOn = null } };

        var table = rows.ToDataTable();

        var row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal(1, row["Id"]);
        Assert.Equal(DBNull.Value, row["Name"]);
        Assert.Equal(DBNull.Value, row["Amount"]);
        Assert.Equal(DBNull.Value, row["CreatedOn"]);
    }

    [Fact]
    public void ToDataTable_DefaultValueTypeValues_AreKeptAsValuesNotNulls()
    {
        var rows = new List<Row> { new() { Id = 0, Name = "", Amount = 0m } };

        var table = rows.ToDataTable();

        var row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal(0, row["Id"]);
        Assert.Equal("", row["Name"]);
        Assert.Equal(0m, row["Amount"]);
    }

    [Fact]
    public void ToDataTable_NullItems_AreSkipped()
    {
        var rows = new List<Row?> { new() { Id = 1 }, null, new() { Id = 2 } };

        var table = rows.ToDataTable();

        Assert.Equal([1, 2], table.Rows.Cast<DataRow>().Select(r => r["Id"]));
    }

    [Fact]
    public void ToDataTable_EmptySequence_StillProducesTheColumns()
    {
        var table = Enumerable.Empty<Row>().ToDataTable();

        Assert.Empty(table.Rows);
        Assert.Equal(4, table.Columns.Count);
    }

    [Fact]
    public void ToDataTable_ComputedAndInheritedProperties_AreIncluded()
    {
        var rows = new List<DerivedRow> { new() { Id = 3, Name = "third" } };

        var table = rows.ToDataTable();

        // Anything publicly readable becomes a column, including a get-only computed property
        // and everything inherited - the destination table has to have matching columns.
        // Compared as a sorted set because reflection does not promise where inherited
        // properties land in the order.
        Assert.Equal(
            ["Amount", "CreatedOn", "Doubled", "Id", "Name"],
            table.Columns.Cast<DataColumn>().Select(c => c.ColumnName).Order());

        var row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal(6, row["Doubled"]);
        Assert.Equal("third", row["Name"]);
    }

    [Fact]
    public void ToDataTable_NonPublicAndStaticMembers_AreIgnored()
    {
        var table = new List<RowWithHiddenMembers>().ToDataTable();

        Assert.Equal(["Visible"], table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
    }

    [Fact]
    public void ToDataTable_CalledRepeatedly_ReturnsIndependentTables()
    {
        // The compiled extractor is cached per type, so a second call must not reuse the first
        // table or leak rows into it.
        var first = new List<Row> { new() { Id = 1 } }.ToDataTable();
        var second = new List<Row> { new() { Id = 2 }, new() { Id = 3 } }.ToDataTable();

        Assert.NotSame(first, second);
        Assert.Equal([1], first.Rows.Cast<DataRow>().Select(r => r["Id"]));
        Assert.Equal([2, 3], second.Rows.Cast<DataRow>().Select(r => r["Id"]));
    }

    [Fact]
    public void ToDataTable_DifferentTypes_GetTheirOwnExtractor()
    {
        // Cache keyed by type: an extractor compiled for one type must never be handed to another.
        var rows = new List<Row> { new() { Id = 1, Name = "one" } }.ToDataTable();
        var others = new List<OtherRow> { new() { Code = "abc" } }.ToDataTable();

        Assert.Equal(["Id", "Name", "Amount", "CreatedOn"], rows.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Equal(["Code"], others.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Equal("abc", Assert.Single(others.Rows.Cast<DataRow>())["Code"]);
    }

    // Test row types are public because Expression.Compile cannot reach members of a non-public type.
    public class Row
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public decimal? Amount { get; set; }
        public DateTime? CreatedOn { get; set; }
    }

    public sealed class DerivedRow : Row
    {
        public int Doubled => Id * 2;
    }

    public sealed class OtherRow
    {
        public string Code { get; set; } = "";
    }

    public sealed class RowWithHiddenMembers
    {
        public static int Static { get; set; }
        public int Visible { get; set; }
        internal int Internal { get; set; }
        private int Private { get; set; }
        public int Field = 0;

        // Keeps the compiler from warning that Private is never read.
        public int ReadPrivate() => Private;
    }
}
