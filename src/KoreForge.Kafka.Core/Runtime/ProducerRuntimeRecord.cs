using System.Threading;

namespace KoreForge.Kafka.Core.Runtime;

public sealed class ProducerRuntimeRecord
{
    private long _totalProducedCount;
    private long _lastProducedLocalTicks;
    private int _lastBatchSize;
    private long _lastBatchDurationTicks;

    public void RecordBatch(int batchSize, TimeSpan duration, DateTime lastProducedLocal)
    {
        if (batchSize < 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if (duration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));

        Interlocked.Add(ref _totalProducedCount, batchSize);
        Volatile.Write(ref _lastBatchSize, batchSize);
        Volatile.Write(ref _lastBatchDurationTicks, duration.Ticks);
        Volatile.Write(ref _lastProducedLocalTicks, lastProducedLocal.Ticks);
    }

    public ProducerRuntimeSnapshot Snapshot
    {
        get
        {
            var total = Interlocked.Read(ref _totalProducedCount);
            var lastTicks = Volatile.Read(ref _lastProducedLocalTicks);
            var batchSize = Volatile.Read(ref _lastBatchSize);
            var batchTicks = Volatile.Read(ref _lastBatchDurationTicks);

            var lastProduced = lastTicks == 0 ? (DateTime?)null : new DateTime(lastTicks, DateTimeKind.Local);
            var duration = batchTicks == 0 ? TimeSpan.Zero : TimeSpan.FromTicks(batchTicks);

            return new ProducerRuntimeSnapshot(total, lastProduced, batchSize, duration);
        }
    }
}

public readonly record struct ProducerRuntimeSnapshot(
    long TotalProducedCount,
    DateTime? LastProducedAtLocal,
    int LastBatchSize,
    TimeSpan LastBatchDuration);
