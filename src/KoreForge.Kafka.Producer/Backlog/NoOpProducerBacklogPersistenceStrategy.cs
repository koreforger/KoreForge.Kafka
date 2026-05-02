using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Abstractions;

namespace KoreForge.Kafka.Producer.Backlog;

public sealed class NoOpProducerBacklogPersistenceStrategy : IProducerBacklogPersistenceStrategy
{
    public static NoOpProducerBacklogPersistenceStrategy Instance { get; } = new();

    private NoOpProducerBacklogPersistenceStrategy()
    {
    }

    public Task PersistAsync(ProducerBacklogPersistContext context, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<OutgoingEnvelope>> RestoreAsync(ProducerBacklogRestoreContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult<IReadOnlyList<OutgoingEnvelope>>(Array.Empty<OutgoingEnvelope>());
    }
}
