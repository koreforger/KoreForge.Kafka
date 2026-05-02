using FluentAssertions;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Configuration.Runtime;
using KoreForge.Kafka.Producer.Backlog;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Backlog;

public sealed class DefaultProducerBacklogPersistenceStrategyFactoryTests
{
    [Fact]
    public void Create_ReturnsFileStrategy_ByDefault()
    {
        var settings = new ExtendedProducerSettings();
        var factory = new DefaultProducerBacklogPersistenceStrategyFactory();

        var strategy = factory.Create(settings, NullLoggerFactory.Instance);

        strategy.Should().BeOfType<FileProducerBacklogPersistenceStrategy>();
    }

    [Fact]
    public void Create_ReturnsNoOp_WhenStrategyIsNone()
    {
        var settings = new ExtendedProducerSettings
        {
            Backlog = new ProducerBacklogSettings { Strategy = ProducerBacklogStrategies.None }
        };
        var factory = new DefaultProducerBacklogPersistenceStrategyFactory();

        var strategy = factory.Create(settings, NullLoggerFactory.Instance);

        strategy.Should().BeSameAs(NoOpProducerBacklogPersistenceStrategy.Instance);
    }

    [Fact]
    public void Create_Throws_WhenStrategyUnknown()
    {
        var settings = new ExtendedProducerSettings
        {
            Backlog = new ProducerBacklogSettings { Strategy = "custom" }
        };
        var factory = new DefaultProducerBacklogPersistenceStrategyFactory();

        var act = () => factory.Create(settings, NullLoggerFactory.Instance);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*custom*");
    }
}
