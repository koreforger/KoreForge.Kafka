using System;
using Microsoft.Extensions.DependencyInjection;

namespace KF.Kafka.Producer.Abstractions;

public interface IProducerSerializerRegistration
{
    void Register(IProducerSerializerRegistry registry, IServiceProvider serviceProvider);
}

internal sealed class ProducerSerializerRegistration<TMessage, TSerializer> : IProducerSerializerRegistration
    where TSerializer : class, IProducerSerializer<TMessage>
{
    public void Register(IProducerSerializerRegistry registry, IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(serviceProvider);

        var serializer = serviceProvider.GetService<TSerializer>()
                         ?? ActivatorUtilities.CreateInstance<TSerializer>(serviceProvider);
        registry.RegisterSerializer(serializer);
    }
}
