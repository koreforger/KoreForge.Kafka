using System;
using FluentAssertions;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Extensions;
using KoreForge.Kafka.Producer.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Serialization;

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
