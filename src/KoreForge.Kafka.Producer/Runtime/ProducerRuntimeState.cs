using System;

namespace KoreForge.Kafka.Producer.Runtime;

public sealed class ProducerRuntimeState
{
    private readonly ProducerRuntimeRecord[] _records;

    public ProducerRuntimeState(int workerCount)
    {
        if (workerCount <= 0) throw new ArgumentOutOfRangeException(nameof(workerCount));
        _records = new ProducerRuntimeRecord[workerCount];
        for (var i = 0; i < workerCount; i++)
        {
            _records[i] = new ProducerRuntimeRecord();
        }
    }

    public int WorkerCount => _records.Length;

    public ProducerRuntimeRecord this[int index]
    {
        get
        {
            if ((uint)index >= _records.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return _records[index];
        }
    }

    public ProducerRuntimeSnapshot[] Snapshot()
    {
        var snapshots = new ProducerRuntimeSnapshot[_records.Length];
        for (var i = 0; i < _records.Length; i++)
        {
            snapshots[i] = _records[i].Snapshot;
        }

        return snapshots;
    }
}
