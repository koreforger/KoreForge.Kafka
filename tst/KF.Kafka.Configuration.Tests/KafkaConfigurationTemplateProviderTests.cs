using FluentAssertions;
using KF.Kafka.Configuration.Templates;
using Xunit;

namespace KF.Kafka.Configuration.Tests;

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
