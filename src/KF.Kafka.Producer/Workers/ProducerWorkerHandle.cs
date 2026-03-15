using System.Threading;
using KF.Kafka.Producer.Runtime;

namespace KF.Kafka.Producer.Workers;

internal sealed class ProducerWorkerHandle
{
    private int _failureCount;

    public ProducerWorkerHandle(int workerId, string topicName, ProducerRuntimeRecord runtimeRecord)
    {
        WorkerId = workerId;
        TopicName = topicName;
        RuntimeRecord = runtimeRecord;
    }

    public int WorkerId { get; }
    public string TopicName { get; }
    public ProducerRuntimeRecord RuntimeRecord { get; }
    public ProducerWorker Worker { get; set; } = null!;
    public bool IsStopping { get; set; }
    public bool CircuitBreakerTripped { get; set; }

    public int IncrementFailures() => Interlocked.Increment(ref _failureCount);

    public void ResetFailures() => Interlocked.Exchange(ref _failureCount, 0);
}
