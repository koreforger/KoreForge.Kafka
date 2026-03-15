using System;
using System.Collections.Generic;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Producer;
using Microsoft.Extensions.Logging;

namespace KF.Kafka.Producer.Backlog;

public interface IProducerBacklogPersistenceStrategyFactory
{
    IProducerBacklogPersistenceStrategy Create(ExtendedProducerSettings settings, ILoggerFactory loggerFactory);
}

public sealed class DefaultProducerBacklogPersistenceStrategyFactory : IProducerBacklogPersistenceStrategyFactory
{
    private readonly Dictionary<string, Func<ILoggerFactory, IProducerBacklogPersistenceStrategy>> _strategies =
        new(StringComparer.OrdinalIgnoreCase);

    public DefaultProducerBacklogPersistenceStrategyFactory()
    {
        Register(ProducerBacklogStrategies.File, factory =>
            new FileProducerBacklogPersistenceStrategy(factory.CreateLogger<FileProducerBacklogPersistenceStrategy>()));
        Register(ProducerBacklogStrategies.None, _ => NoOpProducerBacklogPersistenceStrategy.Instance);
    }

    public DefaultProducerBacklogPersistenceStrategyFactory Register(
        string strategy,
        Func<ILoggerFactory, IProducerBacklogPersistenceStrategy> factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(strategy);
        _strategies[strategy] = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public IProducerBacklogPersistenceStrategy Create(ExtendedProducerSettings settings, ILoggerFactory loggerFactory)
    {
        if (settings == null) throw new ArgumentNullException(nameof(settings));
        if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory));

        var strategyName = settings.Backlog.Strategy ?? ProducerBacklogStrategies.File;
        if (!_strategies.TryGetValue(strategyName, out var factory))
        {
            throw new InvalidOperationException($"Unknown producer backlog strategy '{strategyName}'.");
        }

        return factory(loggerFactory);
    }
}
