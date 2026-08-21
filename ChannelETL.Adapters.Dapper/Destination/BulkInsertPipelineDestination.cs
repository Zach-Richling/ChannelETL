using Microsoft.Data.SqlClient;
using System.Data;

namespace ChannelETL.Adapters.Dapper;

public abstract class BulkInsertPipelineDestination<TDestination>(SqlConnection connection, string tableName, int batchSize) : BatchedPipelineDestination<TDestination>(batchSize)
{
    public override async Task ConsumeBatchAsync(IReadOnlyList<TDestination> batch, CancellationToken token)
    {
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(token);
        }

        var data = batch.ToDataTable();

        var options = SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction;
        using var bulkCopy = new SqlBulkCopy(connection, options, null)
        {
            DestinationTableName = tableName,
            BulkCopyTimeout = 0
        };

        foreach (DataColumn column in data.Columns)
        {
            var colName = column.ColumnName;
            bulkCopy.ColumnMappings.Add(colName, colName);
        }

        await bulkCopy.WriteToServerAsync(data, token);
    }
}
