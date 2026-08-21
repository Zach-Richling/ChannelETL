using Microsoft.Extensions.DependencyInjection;

namespace ChannelETL.Tests;

public class IServiceCollectionExtensionsTests
{
    [Fact]
    public void AddPipelinesFromAssembly_RegistersConcreteTypeAndItsInterfaces_AsSameScopedInstance()
    {
        var services = new ServiceCollection();
        services.AddPipelinesFromAssembly(typeof(IServiceCollectionExtensionsTests).Assembly);
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        var concrete = scope.ServiceProvider.GetRequiredService<FixtureSource>();
        var asInterface = scope.ServiceProvider.GetRequiredService<IPipelineSource<int>>();

        Assert.Same(concrete, asInterface);
    }

    [Fact]
    public void AddPipelinesFromAssembly_RegistersScoped_NewInstancePerScope()
    {
        var services = new ServiceCollection();
        services.AddPipelinesFromAssembly(typeof(IServiceCollectionExtensionsTests).Assembly);
        using var provider = services.BuildServiceProvider();

        using var scopeA = provider.CreateScope();
        using var scopeB = provider.CreateScope();

        var instanceA = scopeA.ServiceProvider.GetRequiredService<FixtureSource>();
        var instanceB = scopeB.ServiceProvider.GetRequiredService<FixtureSource>();

        Assert.NotSame(instanceA, instanceB);
    }

    // Implements IPipelineSource<int> specifically so identity checks against that interface
    // are unambiguous - unlike bare IPipeline, no other fixture in this assembly closes over int here.
    private class FixtureSource : IPipelineSource<int>
    {
        public IAsyncEnumerable<int> ProduceAsync(CancellationToken token) => AsyncEnumerable.Empty<int>();
    }
}
