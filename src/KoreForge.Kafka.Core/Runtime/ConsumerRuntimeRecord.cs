using System;
using System.Threading;

namespace KoreForge.Kafka.Core.Runtime;

public sealed class ConsumerRuntimeRecord
{
    private long _totalConsumedCount;
    private long _lastConsumedLocalTicks;
    private int _lastBatchSize;
    private long _lastBatchProcessingTicks;
    private int _backlogSize;
    private int _backlogCapacity;
    private int _state = (int)ConsumerWorkerState.Stopped;

    public void RecordBatch(int batchSize, TimeSpan processingDuration, DateTime lastConsumedLocal)
    {
        if (batchSize < 0) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if (processingDuration < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(processingDuration));

        Interlocked.Add(ref _totalConsumedCount, batchSize);
        Volatile.Write(ref _lastBatchSize, batchSize);
        Volatile.Write(ref _lastBatchProcessingTicks, processingDuration.Ticks);
        Volatile.Write(ref _lastConsumedLocalTicks, lastConsumedLocal.Ticks);
        UpdateState(ConsumerWorkerState.Active);
    }

    public void RecordBacklog(int backlogSize, int backlogCapacity)
    {
        Volatile.Write(ref _backlogSize, backlogSize);
        Volatile.Write(ref _backlogCapacity, backlogCapacity);
    }

    public void UpdateState(ConsumerWorkerState state)
    {
        Volatile.Write(ref _state, (int)state);
    }

    public ConsumerRuntimeSnapshot Snapshot
    {
        get
        {
            var count = Interlocked.Read(ref _totalConsumedCount);
            var ticks = Volatile.Read(ref _lastConsumedLocalTicks);
            var batchSize = Volatile.Read(ref _lastBatchSize);
            var batchTicks = Volatile.Read(ref _lastBatchProcessingTicks);

            var lastConsumed = ticks == 0 ? (DateTime?)null : new DateTime(ticks, DateTimeKind.Local);
            var duration = batchTicks == 0 ? TimeSpan.Zero : TimeSpan.FromTicks(batchTicks);

            var backlogSize = Volatile.Read(ref _backlogSize);
            var backlogCapacity = Volatile.Read(ref _backlogCapacity);
            var state = (ConsumerWorkerState)Volatile.Read(ref _state);

            return new ConsumerRuntimeSnapshot(count, lastConsumed, batchSize, duration, state, backlogSize, backlogCapacity);
        }
    }
}

public readonly record struct ConsumerRuntimeSnapshot(
    long TotalConsumedCount,
    DateTime? LastConsumedAtLocal,
    int LastBatchSize,
    TimeSpan LastBatchDuration,
    ConsumerWorkerState State,
    int BacklogSize,
    int BacklogCapacity);

public enum ConsumerWorkerState
{
    Stopped = 0,
    Starting = 1,
    Active = 2,
    Paused = 3,
    Failed = 4
}
