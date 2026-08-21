using Microsoft.Extensions.Logging.Abstractions;

namespace ChannelETL.Tests;

internal static class TestHelpers
{
    public static PipelineExecutionContext CreateContext(IEnumerable<IPipeline> parentPipelines, CancellationToken? token = null) => new()
    {
        ParentPipelines = parentPipelines,
        Logger = NullLogger.Instance,
        Token = token ?? CancellationToken.None
    };
}
