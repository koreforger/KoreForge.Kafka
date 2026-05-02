using KoreForge.Kafka.AdminClient.Abstractions;
using KoreForge.Kafka.AdminClient.Instrumentation;
using KoreForge.Kafka.AdminClient.Internal;
using KoreForge.Kafka.Configuration.Admin;
using KoreForge.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace KoreForge.Kafka.AdminClient.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaAdminClient(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "Kafka:Admin")
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<KafkaAdminOptions>(configuration.GetSection(sectionName));
        services.AddSingleton<IValidateOptions<KafkaAdminOptions>, KafkaAdminOptionsValidator>();

        services.TryAddSingleton<IKafkaAdminMetrics>(sp =>
        {
            var monitor = sp.GetService<IOperationMonitor>();
            return monitor is null
                ? NoOpKafkaAdminMetrics.Instance
                : new KafkaAdminOperationMonitorMetrics(monitor);
        });
        services.AddSingleton<IKafkaAdminClient, KafkaAdminClient>();

        return services;
    }
}
