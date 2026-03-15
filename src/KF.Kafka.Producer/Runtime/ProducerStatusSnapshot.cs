using System;
using System.Collections.Generic;

namespace KF.Kafka.Producer.Runtime;

public sealed class ProducerStatusSnapshot
{
    public int TotalWorkers { get; init; }
    public long TotalBufferedMessages { get; init; }
    public long TotalBufferCapacity { get; init; }
    public bool CircuitBreakerTripped { get; init; }
    public IReadOnlyList<TopicBufferStatus> Topics { get; init; } = Array.Empty<TopicBufferStatus>();
    public IReadOnlyList<ProducerRuntimeSnapshot> RuntimeRecords { get; init; } = Array.Empty<ProducerRuntimeSnapshot>();
}

public sealed class TopicBufferStatus
{
    public string TopicName { get; init; } = string.Empty;
    public long BufferedMessages { get; init; }
    public long BufferCapacity { get; init; }
    public double BufferFillPercentage => BufferCapacity == 0 ? 0 : (double)BufferedMessages / BufferCapacity * 100;
    public ProducerBacklogTopicStatus? Backlog { get; init; }
}

public sealed class ProducerBacklogTopicStatus
{
    public bool PersistenceEnabled { get; init; }
    public long PersistedMessages { get; init; }
    public long PersistedBytes { get; init; }
    public DateTimeOffset? LastPersistedAt { get; init; }
    public bool ArchiveOnRestore { get; init; }
}
