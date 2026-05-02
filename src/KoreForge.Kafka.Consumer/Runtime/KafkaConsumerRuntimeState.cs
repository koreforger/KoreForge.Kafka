using System;
using System.Collections.Generic;
using System.Linq;
using KoreForge.Kafka.Core.Runtime;

namespace KoreForge.Kafka.Consumer.Runtime;

/// <summary>
/// Lock-free container recording per-worker runtime statistics.
/// </summary>
public sealed class KafkaConsumerRuntimeState
{
    private readonly ConsumerRuntimeRecord[] _records;

    public KafkaConsumerRuntimeState(int workerCount)
    {
        if (workerCount < 1) throw new ArgumentOutOfRangeException(nameof(workerCount));
        _records = new ConsumerRuntimeRecord[workerCount];
        for (var i = 0; i < workerCount; i++)
        {
            _records[i] = new ConsumerRuntimeRecord();
        }
    }

    public ConsumerRuntimeRecord this[int workerIndex] => _records[workerIndex];

    public ConsumersStatusSnapshot CreateSnapshot() => new(
        _records.Select(r => r.Snapshot).ToArray());
}

public sealed record ConsumersStatusSnapshot(IReadOnlyList<ConsumerRuntimeSnapshot> Workers)
{
    public long TotalConsumed => Workers.Sum(w => w.TotalConsumedCount);
    public int PausedWorkerCount => Workers.Count(w => w.State == ConsumerWorkerState.Paused);
    public double PercentagePausedConsumers => Workers.Count == 0 ? 0 : (double)PausedWorkerCount / Workers.Count * 100;
    public bool AnyWorkerFailed => Workers.Any(w => w.State == ConsumerWorkerState.Failed);
    public int FailedWorkerCount => Workers.Count(w => w.State == ConsumerWorkerState.Failed);
}
