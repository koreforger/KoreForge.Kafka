using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KF.Kafka.Producer.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaProducerSerializer<TMessage, TSerializer>(this IServiceCollection services)
        where TSerializer : class, IProducerSerializer<TMessage>
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddTransient<TSerializer>();
        services.AddSingleton<IProducerSerializerRegistration, ProducerSerializerRegistration<TMessage, TSerializer>>();
        return services;
    }

    public static IServiceCollection AddKafkaProducerJsonSerializer<TMessage>(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddKafkaProducerSerializer<TMessage, JsonProducerSerializer<TMessage>>();
    }
}
