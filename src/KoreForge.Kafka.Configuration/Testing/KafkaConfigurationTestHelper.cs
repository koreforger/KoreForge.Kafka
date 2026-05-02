using System.IO;
using System.Text;
using KoreForge.Kafka.Configuration.Extensions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Configuration.Validation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KoreForge.Kafka.Configuration.Testing;

public static class KafkaConfigurationTestHelper
{
    public static ServiceProvider BuildServiceProviderFromJson(string json, string sectionName = "Kafka")
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json)))
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKafkaConfiguration(configuration, sectionName);

        return services.BuildServiceProvider();
    }

    public static IKafkaClientConfigFactory CreateFactoryFromJson(string json, string sectionName = "Kafka")
    {
        var provider = BuildServiceProviderFromJson(json, sectionName);
        return provider.GetRequiredService<IKafkaClientConfigFactory>();
    }

    public static (IKafkaClientConfigFactory Factory, IKafkaConfigValidator Validator) CreateFactoryAndValidatorFromJson(
        string json,
        string sectionName = "Kafka")
    {
        var provider = BuildServiceProviderFromJson(json, sectionName);
        return (
            provider.GetRequiredService<IKafkaClientConfigFactory>(),
            provider.GetRequiredService<IKafkaConfigValidator>());
    }
}
