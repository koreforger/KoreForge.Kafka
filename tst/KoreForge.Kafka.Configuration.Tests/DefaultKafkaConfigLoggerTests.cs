using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Logging;
using Xunit;

namespace KoreForge.Kafka.Configuration.Tests;

public class DefaultKafkaConfigLoggerTests
{
    [Fact]
    public void ToLogString_MasksSensitiveValues()
    {
        var logger = new DefaultKafkaConfigLogger();
        var config = new ClientConfig
        {
            BootstrapServers = "localhost:9092",
            SaslPassword = "secret"
        };

        var text = logger.ToLogString(config);

        text.Should().Contain("sasl.password=***MASKED***");
        text.Should().Contain("bootstrap.servers=localhost:9092");
    }

    [Fact]
    public void ToLogProperties_PrefixesKeys()
    {
        var logger = new DefaultKafkaConfigLogger();
        var config = new ClientConfig
        {
            BootstrapServers = "localhost:9092",
            SaslPassword = "secret"
        };

        var properties = logger.ToLogProperties(config);

        properties.Should().ContainKey("kafka.bootstrap.servers");
        properties["kafka.bootstrap.servers"].Should().Be("localhost:9092");
        properties["kafka.sasl.password"].Should().Be("***MASKED***");
    }
}
