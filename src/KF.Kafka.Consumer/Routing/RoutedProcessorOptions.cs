using System;

namespace KF.Kafka.Consumer.Routing;

/// <summary>
/// Configuration options for the routed batch processor.
/// </summary>
public sealed class RoutedProcessorOptions
{
    /// <summary>
    /// Number of consumer threads reading from Kafka.
    /// Should typically match or exceed the Kafka partition count for the subscribed topics.
    /// Default: Environment.ProcessorCount
    /// </summary>
    public int ConsumerThreadCount { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Number of processing buckets. Must be a power of 2.
    /// Determines the parallelism of message processing.
    /// Default: 64
    /// </summary>
    public int BucketCount { get; set; } = 64;

    /// <summary>
    /// Queue capacity per bucket. Messages queue here while waiting for the bucket worker.
    /// Default: 50,000
    /// </summary>
    public int BucketQueueCapacity { get; set; } = 50_000;

    /// <summary>
    /// Maximum batch size delivered to the bucket processor.
    /// Smaller batches reduce latency; larger batches improve throughput.
    /// Default: 1000
    /// </summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Maximum time to wait for a batch to fill before processing.
    /// Default: 100ms
    /// </summary>
    public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Behavior when bucket queues fill up.
    /// Default: PauseConsumption
    /// </summary>
    public BackpressureBehavior BackpressureBehavior { get; set; } = BackpressureBehavior.PauseConsumption;

    /// <summary>
    /// Queue fill percentage (0-100) that triggers backpressure.
    /// Default: 80
    /// </summary>
    public int BackpressureThresholdPercent { get; set; } = 80;

    /// <summary>
    /// Queue fill percentage (0-100) that releases backpressure.
    /// Must be less than BackpressureThresholdPercent.
    /// Default: 50
    /// </summary>
    public int BackpressureResumePercent { get; set; } = 50;

    /// <summary>
    /// Validates the options and throws ArgumentException if invalid.
    /// </summary>
    public void Validate()
    {
        if (ConsumerThreadCount < 1)
            throw new ArgumentOutOfRangeException(nameof(ConsumerThreadCount), "Must be at least 1");

        if (BucketCount < 1)
            throw new ArgumentOutOfRangeException(nameof(BucketCount), "Must be at least 1");

        if (!IsPowerOfTwo(BucketCount))
            throw new ArgumentException("BucketCount must be a power of 2", nameof(BucketCount));

        if (BucketQueueCapacity < 1)
            throw new ArgumentOutOfRangeException(nameof(BucketQueueCapacity), "Must be at least 1");

        if (MaxBatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(MaxBatchSize), "Must be at least 1");

        if (BatchTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(BatchTimeout), "Must be positive");

        if (BackpressureThresholdPercent is < 1 or > 100)
            throw new ArgumentOutOfRangeException(nameof(BackpressureThresholdPercent), "Must be between 1 and 100");

        if (BackpressureResumePercent is < 0 or > 99)
            throw new ArgumentOutOfRangeException(nameof(BackpressureResumePercent), "Must be between 0 and 99");

        if (BackpressureResumePercent >= BackpressureThresholdPercent)
            throw new ArgumentException("BackpressureResumePercent must be less than BackpressureThresholdPercent");
    }

    private static bool IsPowerOfTwo(int value) => value > 0 && (value & (value - 1)) == 0;
}

/// <summary>
/// Behavior when bucket queues fill up.
/// </summary>
public enum BackpressureBehavior
{
    /// <summary>
    /// Pause Kafka consumption until queues drain below the resume threshold.
    /// Ensures no data loss but may cause consumer lag.
    /// </summary>
    PauseConsumption,

    /// <summary>
    /// Drop messages when queues are full.
    /// Records dropped message metrics for monitoring.
    /// </summary>
    DropMessages,

    /// <summary>
    /// Block the consumer thread until space is available.
    /// May cause consumer heartbeat timeout if prolonged.
    /// </summary>
    BlockUntilSpace
}
