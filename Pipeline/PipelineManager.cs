namespace ChannelETL.Pipeline;

public class PipelineManager
{
    private List<IPipeline> _pipelines = [];

    public async Task RunAsync(CancellationToken token)
    {
        try
        {
            await Task.WhenAll(_pipelines.Select(x => x.RunAsync(token)));
        }
        catch (Exception)
        {
            //TODO: log here?
        }
    }

    public void AddPipeline(IPipeline pipeline)
    {
        if (!_pipelines.Contains(pipeline))
        {
            _pipelines.Add(pipeline);
        }
    }
}
