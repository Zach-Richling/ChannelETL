namespace ChannelETL;

/// <summary>
/// A base class for pipeline destinations that consume items in batches
/// </summary>
public abstract class BatchedPipelineDestination<TDest> : IPipelineDestination<TDest>
{
    private readonly int _batchSize = 10;
    private readonly List<TDest> _batch;

    //Making this thread-safe to future-proof the class in case of concurrent calls to ConsumeAsync or CompleteAsync
    //but it shouldn't matter currently since the pipeline is single-threaded
    private readonly SemaphoreSlim _lock = new(1, 1);

    public BatchedPipelineDestination()
    {
        _batch = new List<TDest>(_batchSize);
    }

    public BatchedPipelineDestination(int batchSize)
    {
        _batchSize = batchSize;
        _batch = new List<TDest>(batchSize);
    }

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

    /// <summary>
    /// Completes the pipeline and flushes any remaining items in the batch.
    /// You must call the base method if implementing a custom destination to ensure that all items are processed.
    /// </summary>
    public virtual async Task CompleteAsync(CancellationToken token)
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
        try
        {
            await ConsumeBatchAsync(_batch, token);
        }
        finally
        {
            _batch.Clear();
        }
    }

    /// <summary>
    /// Consumes a batch of items. This method is called when the batch size is reached or when the pipeline is completed.
    /// </summary>
    public abstract Task ConsumeBatchAsync(IReadOnlyList<TDest> batch, CancellationToken token);
}
