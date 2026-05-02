using FluentAssertions;
using KoreForge.Kafka.Configuration.Templates;
using Xunit;

namespace KoreForge.Kafka.Configuration.Tests;

public class KafkaConfigurationTemplateProviderTests
{
    [Fact]
    public void GenerateKafkaJsonTemplate_ReturnsIndentedJson()
    {
        var provider = new KafkaConfigurationTemplateProvider();

        var json = provider.GenerateKafkaJsonTemplate();

        json.Should().Contain("\"Kafka\"");
        json.Should().Contain("PaymentsConsumer");
    }
}
