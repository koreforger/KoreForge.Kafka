using System;
using System.Threading;

namespace KoreForge.Kafka.Producer.Runtime;

public sealed class ProducerRuntimeRecord
{
    private long _totalProduced;
    private long _lastProducedTicks;
    private int _lastBatchSize;
    private long _lastBatchDurationTicks;
    private int _status;

    public void RecordBatch(int batchSize, TimeSpan duration, DateTime lastProducedLocal)
    {
        if (batchSize < 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if (duration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(duration));

        Interlocked.Add(ref _totalProduced, batchSize);
        Volatile.Write(ref _lastBatchSize, batchSize);
        Volatile.Write(ref _lastBatchDurationTicks, duration.Ticks);
        Volatile.Write(ref _lastProducedTicks, lastProducedLocal.Ticks);
        Volatile.Write(ref _status, (int)ProducerWorkerState.Running);
    }

    public void RecordFailure()
    {
        Volatile.Write(ref _status, (int)ProducerWorkerState.Failed);
    }

    public void RecordStopped()
    {
        Volatile.Write(ref _status, (int)ProducerWorkerState.Stopped);
    }

    public ProducerRuntimeSnapshot Snapshot
    {
        get
        {
            var produced = Interlocked.Read(ref _totalProduced);
            var lastTicks = Volatile.Read(ref _lastProducedTicks);
            var batchSize = Volatile.Read(ref _lastBatchSize);
            var batchTicks = Volatile.Read(ref _lastBatchDurationTicks);
            var status = (ProducerWorkerState)Volatile.Read(ref _status);

            var lastProduced = lastTicks == 0 ? (DateTime?)null : new DateTime(lastTicks, DateTimeKind.Local);
            var duration = batchTicks == 0 ? TimeSpan.Zero : TimeSpan.FromTicks(batchTicks);
            return new ProducerRuntimeSnapshot(produced, lastProduced, batchSize, duration, status);
        }
    }
}

public readonly record struct ProducerRuntimeSnapshot(
    long TotalProducedCount,
    DateTime? LastProducedAtLocal,
    int LastBatchSize,
    TimeSpan LastBatchDuration,
    ProducerWorkerState State);

public enum ProducerWorkerState
{
    Unknown = 0,
    Running = 1,
    Failed = 2,
    Stopped = 3
}
