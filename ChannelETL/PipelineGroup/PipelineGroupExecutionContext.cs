using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ChannelETL;

public readonly struct PipelineGroupExecutionContext()
{
    public required IServiceScopeFactory ScopeFactory { get; init; }
    public required ILogger Logger { get; init; }
    public required CancellationToken Token { get; init; }
};
