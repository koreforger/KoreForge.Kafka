using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Configuration.Generation;
using KoreForge.Kafka.Configuration.Logging;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Profiles;
using KoreForge.Kafka.Configuration.Security;
using KoreForge.Kafka.Configuration.Templates;
using KoreForge.Kafka.Configuration.Validation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KoreForge.Kafka.Configuration.Extensions;

public static class KafkaConfigurationServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaConfiguration(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "Kafka")
    {
        services.Configure<KafkaConfigurationRootOptions>(configuration.GetSection(sectionName));

        services.AddSingleton<IKafkaGroupIdGenerator, DefaultKafkaGroupIdGenerator>();
        services.AddSingleton<IKafkaConfigLogger, DefaultKafkaConfigLogger>();
        services.AddSingleton<IKafkaCertificateStore, FileKafkaCertificateStore>();
        services.AddSingleton<IKafkaSecurityProvider, DefaultKafkaSecurityProvider>();
        services.AddSingleton<IKafkaProfileCatalog, KafkaProfileCatalog>();
        services.AddSingleton<IKafkaConfigurationChangeNotifier, KafkaConfigurationChangeNotifier>();
        services.AddSingleton<IKafkaClientConfigFactory, KafkaClientConfigFactory>();
        services.AddSingleton<IKafkaConfigValidator, KafkaConfigValidator>();
        services.AddSingleton<IKafkaConfigurationTemplateProvider, KafkaConfigurationTemplateProvider>();

        return services;
    }
}
