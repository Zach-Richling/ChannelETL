using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace ChannelETL.Extensions.DependencyInjection;

public static class IServiceCollectionExtensions
{
    /// <summary>
    /// Adds all classes that implement the pipeline interfaces to the service collection from the specified assembly.
    /// </summary>
    public static IServiceCollection AddPipelinesFromAssembly(this IServiceCollection services, Assembly assembly)
    {
        var classes = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract);

        var pipelineInterfaces = new List<Type>()
        {
            typeof(IPipeline),
            typeof(IPipelineGroup),
            typeof(IPipelineSource<>),
            typeof(IPipelineTransformation<,>),
            typeof(IPipelineDestination<>)
        };

        //Look for all classes that implement pipeline interfaces
        foreach (var classType in classes.Where(t => pipelineInterfaces.Any(i => t.IsAssignableTo(i))))
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
}
