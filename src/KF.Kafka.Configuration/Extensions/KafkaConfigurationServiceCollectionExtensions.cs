using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Generation;
using KF.Kafka.Configuration.Logging;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Profiles;
using KF.Kafka.Configuration.Security;
using KF.Kafka.Configuration.Templates;
using KF.Kafka.Configuration.Validation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KF.Kafka.Configuration.Extensions;

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
