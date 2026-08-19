using Microsoft.Extensions.Logging;

namespace ChannelETL;

public record PipelineExecutionContext(IEnumerable<IPipeline> ParentPipelines, ILogger Logger, CancellationToken Token);
