using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Producer.Abstractions;

namespace KF.Kafka.Producer.Backlog;

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
