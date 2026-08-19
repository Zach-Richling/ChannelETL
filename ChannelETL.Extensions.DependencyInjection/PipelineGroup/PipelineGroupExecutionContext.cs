using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ChannelETL.Extensions.DependencyInjection;

public record PipelineGroupExecutionContext(IServiceScopeFactory ScopeFactory, ILogger Logger, CancellationToken Token);
