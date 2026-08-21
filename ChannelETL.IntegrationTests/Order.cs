namespace ChannelETL.IntegrationTests;

/// <summary>
/// Row shape for dbo.Orders. A record so collections of orders compare by value in assertions;
/// the settable properties are what Dapper maps onto.
/// </summary>
public sealed record Order
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}
