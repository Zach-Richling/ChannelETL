namespace ChannelETL;

/// <summary>
/// A base class for pipeline destinations that consume items in batches
/// </summary>
public abstract class BatchedPipelineDestination<TDest> : IPipelineDestination<TDest>
{
    private readonly int _batchSize = 10;
    private List<TDest> _batch = [];

    //Making this thread-safe to future-proof the class in case of concurrent calls to ConsumeAsync or CompleteAsync
    //but it shouldn't matter currently since the pipeline is single-threaded
    private readonly SemaphoreSlim _lock = new(1, 1);

    public BatchedPipelineDestination() { }
    public BatchedPipelineDestination(int batchSize)
        => _batchSize = batchSize;

    async Task IPipelineDestination<TDest>.ConsumeAsync(TDest item, CancellationToken token)
    {
        await _lock.WaitAsync(token);

        try
        {
            _batch.Add(item);

            if (_batch.Count >= _batchSize)
            {
                await FlushBatchAsync(token);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    async Task IPipelineDestination<TDest>.CompleteAsync(CancellationToken token)
    {
        await _lock.WaitAsync(token);

        try
        {
            if (_batch.Count > 0)
            {
                await FlushBatchAsync(token);
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task FlushBatchAsync(CancellationToken token)
    {
        var batchToProcess = _batch;
        _batch = new List<TDest>(_batchSize);

        await ConsumeBatchAsync(batchToProcess, token);
    }

    /// <summary>
    /// Consumes a batch of items. This method is called when the batch size is reached or when the pipeline is completed.
    /// </summary>
    public abstract Task ConsumeBatchAsync(IEnumerable<TDest> batch, CancellationToken token);
}
