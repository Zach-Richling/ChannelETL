using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace ChannelETL;

public static class IServiceCollectionExtensions
{
    private static readonly HashSet<Type> PipelineInterfaceDefinitions =
    [
        typeof(IPipeline),
        typeof(IPipelineGroup),
        typeof(IPipelineSource<>),
        typeof(IPipelineTransformation<,>),
        typeof(IPipelineDestination<>)
    ];

    /// <summary>
    /// Adds all classes that implement the pipeline interfaces to the service collection from the specified assembly.
    /// </summary>
    public static IServiceCollection AddPipelinesFromAssembly(this IServiceCollection services, Assembly assembly)
    {
        var classes = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract);

        //Look for all classes that implement pipeline interfaces
        foreach (var classType in classes.Where(ImplementsAnyPipelineInterface))
        {
            //Add the concrete class and all of its interfaces
            services.AddScoped(classType);
            foreach (var interfaceType in classType.GetInterfaces())
            {
                services.AddScoped(interfaceType, services => services.GetRequiredService(classType));
            }
        }

        return services;
    }

    private static bool ImplementsAnyPipelineInterface(Type classType) =>
        classType.GetInterfaces().Any(i => PipelineInterfaceDefinitions.Contains(i.IsGenericType ? i.GetGenericTypeDefinition() : i));
}
