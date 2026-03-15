using System;
using FluentAssertions;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Extensions;
using KF.Kafka.Producer.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace KF.Kafka.Producer.Tests.Serialization;

public sealed class SerializerRegistrationExtensionsTests
{
    [Fact]
    public void AddKafkaProducerJsonSerializer_RegistersSerializer()
    {
        var services = new ServiceCollection();
        services.AddKafkaProducerJsonSerializer<TestMessage>();
        var provider = services.BuildServiceProvider();

        var registry = new ProducerSerializerRegistry();
        foreach (var registration in provider.GetServices<IProducerSerializerRegistration>())
        {
            registration.Register(registry, provider);
        }

        var envelope = new OutgoingEnvelope(new TestMessage("abc"), typeof(TestMessage), "topic-a", null, null, DateTimeOffset.UtcNow);
        var record = registry.Serialize(envelope);

        record.ValueBytes.Should().NotBeNull();
        record.Topic.Should().Be("topic-a");
    }

    private sealed record TestMessage(string Value);
}
