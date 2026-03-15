using System.Collections.Generic;
using System.Text.Json;
using FluentAssertions;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Serialization;
using Xunit;

namespace KF.Kafka.Producer.Tests.Serialization;

public sealed class JsonProducerSerializerTests
{
    [Fact]
    public void Serialize_EncodesValueKeyAndHeaders()
    {
        var serializer = new JsonProducerSerializer<TestMessage>(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        var envelope = new OutgoingEnvelope(
            new TestMessage("value"),
            typeof(TestMessage),
            topic: "topic-a",
            key: "key-1",
            headers: new Dictionary<string, string> { ["trace-id"] = "abc" },
            localCreatedAt: System.DateTimeOffset.UtcNow);

        var record = serializer.Serialize((TestMessage)envelope.Payload, envelope);

        record.Topic.Should().Be("topic-a");
        record.KeyBytes.Should().NotBeNull();
        System.Text.Encoding.UTF8.GetString(record.KeyBytes!).Should().Be("key-1");
        System.Text.Encoding.UTF8.GetString(record.ValueBytes).Should().Contain("value");
        record.Headers.Should().ContainKey("trace-id");
        System.Text.Encoding.UTF8.GetString(record.Headers!["trace-id"]).Should().Be("abc");
    }

    private sealed record TestMessage(string Value);
}
