using System;
using System.Collections.Generic;

namespace KF.Kafka.Consumer.Routing;

/// <summary>
/// Runtime statistics for the routed processor.
/// </summary>
public sealed record RoutedProcessorStats
{
    /// <summary>
    /// Total messages received from Kafka across all consumer threads.
    /// </summary>
    public long TotalMessagesReceived { get; init; }

    /// <summary>
    /// Total messages successfully routed to buckets.
    /// </summary>
    public long TotalMessagesRouted { get; init; }

    /// <summary>
    /// Total messages discarded (e.g., parse failures, no routing key).
    /// </summary>
    public long TotalMessagesDiscarded { get; init; }

    /// <summary>
    /// Total messages dropped due to backpressure (when DropMessages behavior is used).
    /// </summary>
    public long TotalMessagesDropped { get; init; }

    /// <summary>
    /// Per-bucket statistics.
    /// </summary>
    public IReadOnlyList<BucketStats> Buckets { get; init; } = Array.Empty<BucketStats>();
}

/// <summary>
/// Statistics for a single processing bucket.
/// </summary>
public sealed record BucketStats
{
    /// <summary>
    /// Index of this bucket (0 to BucketCount-1).
    /// </summary>
    public int BucketIndex { get; init; }

    /// <summary>
    /// Current number of messages waiting in the queue.
    /// </summary>
    public int QueueDepth { get; init; }

    /// <summary>
    /// Maximum queue capacity.
    /// </summary>
    public int QueueCapacity { get; init; }

    /// <summary>
    /// Current fill percentage (0-100).
    /// </summary>
    public int FillPercent { get; init; }

    /// <summary>
    /// Total messages processed by this bucket's worker.
    /// </summary>
    public long MessagesProcessed { get; init; }

    /// <summary>
    /// Total batches processed by this bucket's worker.
    /// </summary>
    public long BatchesProcessed { get; init; }
}

/// <summary>
/// Event args for backpressure state changes.
/// </summary>
public sealed class BackpressureEventArgs : EventArgs
{
    public BackpressureEventArgs(bool isActive, int maxFillPercent)
    {
        IsActive = isActive;
        MaxFillPercent = maxFillPercent;
        Timestamp = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// True if backpressure is now active, false if released.
    /// </summary>
    public bool IsActive { get; }

    /// <summary>
    /// Maximum fill percentage across all buckets when this event occurred.
    /// </summary>
    public int MaxFillPercent { get; }

    /// <summary>
    /// When this event occurred.
    /// </summary>
    public DateTimeOffset Timestamp { get; }
}
