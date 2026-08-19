using Microsoft.Extensions.Logging;

namespace ChannelETL;

public readonly struct PipelineExecutionContext()
{
    public required IEnumerable<IPipeline> ParentPipelines { get; init; }
    public required ILogger Logger { get; init; }
    public required CancellationToken Token { get; init; }
};
