using System;

namespace KF.Kafka.Consumer.Execution;

public sealed class KafkaBatchProcessingTimeoutException : TimeoutException
{
    public KafkaBatchProcessingTimeoutException(int workerId, int batchSize, TimeSpan timeout)
        : base($"Kafka consumer worker {workerId} batch of {batchSize} records exceeded the processing timeout of {timeout}.")
    {
        WorkerId = workerId;
        BatchSize = batchSize;
        Timeout = timeout;
    }

    public int WorkerId { get; }
    public int BatchSize { get; }
    public TimeSpan Timeout { get; }
}
