using System;
using System.Text;
using FluentAssertions;
using KF.Kafka.Producer.Abstractions;
using Xunit;

namespace KF.Kafka.Producer.Tests.Serialization;

public sealed class ProducerSerializerRegistryTests
{
    [Fact]
    public void Serialize_Throws_WhenSerializerMissing()
    {
        var registry = new ProducerSerializerRegistry();
        var envelope = new OutgoingEnvelope("payload", typeof(string), "topic", null, null, DateTimeOffset.UtcNow);

        Action action = () => registry.Serialize(envelope);

        action.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void Serialize_UsesRegisteredSerializer()
    {
        var registry = new ProducerSerializerRegistry();
        registry.RegisterSerializer(new TestSerializer());
        var envelope = new OutgoingEnvelope("payload", typeof(string), "topic-a", "key", null, DateTimeOffset.UtcNow);

        var record = registry.Serialize(envelope);

        record.Topic.Should().Be("topic-a");
        record.ValueBytes.Should().Equal(Encoding.UTF8.GetBytes("payload"));
        record.KeyBytes.Should().Equal(new byte[] { 1, 2, 3 });
    }

    private sealed class TestSerializer : IProducerSerializer<string>
    {
        public SerializedRecord Serialize(string message, OutgoingEnvelope envelope)
        {
            return new SerializedRecord
            {
                Topic = envelope.Topic,
                KeyBytes = new byte[] { 1, 2, 3 },
                ValueBytes = Encoding.UTF8.GetBytes(message)
            };
        }
    }
}
