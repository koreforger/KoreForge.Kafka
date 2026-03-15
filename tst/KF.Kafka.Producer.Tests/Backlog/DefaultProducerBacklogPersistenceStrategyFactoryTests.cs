using FluentAssertions;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Producer.Backlog;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KF.Kafka.Producer.Tests.Backlog;

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
