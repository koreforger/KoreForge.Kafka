using System;
using Confluent.Kafka;

namespace KF.Kafka.Consumer.Execution;

public sealed class KafkaFatalConsumerException : Exception
{
    public KafkaFatalConsumerException(int workerId, Error error)
        : base($"Kafka consumer worker {workerId} received a fatal Kafka error: {error.Reason}")
    {
        WorkerId = workerId;
        Error = error;
    }

    public int WorkerId { get; }
    public Error Error { get; }
}
