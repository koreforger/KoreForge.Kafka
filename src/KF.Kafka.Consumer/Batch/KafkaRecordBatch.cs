using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace KF.Kafka.Consumer.Batch;

/// <summary>
/// Represents a contiguous batch of Kafka records delivered to a processor.
/// </summary>
public sealed class KafkaRecordBatch
{
    public KafkaRecordBatch(
        IReadOnlyList<ConsumeResult<byte[], byte[]>> records,
        DateTimeOffset createdAt)
    {
        Records = records ?? throw new ArgumentNullException(nameof(records));
        CreatedAt = createdAt;
    }

    public IReadOnlyList<ConsumeResult<byte[], byte[]>> Records { get; }

    /// <summary>
    /// Timestamp captured when the batch left the consumer loop.
    /// Use <see cref="DateTimeOffset.UtcDateTime"/> when a UTC value is needed.
    /// </summary>
    public DateTimeOffset CreatedAt { get; }

    public int Count => Records.Count;
}
