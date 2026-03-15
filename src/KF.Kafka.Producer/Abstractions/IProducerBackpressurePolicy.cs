using System;
using KF.Kafka.Configuration.Producer;

namespace KF.Kafka.Producer.Abstractions;

public readonly record struct ProducerBackpressureContext(
    string TopicName,
    long BufferedMessages,
    int BufferCapacity,
    DateTimeOffset ObservedAt,
    ProducerBackpressureMode Mode);

public readonly record struct ProducerBackpressureDecision(
    bool ShouldBlock,
    bool ShouldDrop,
    bool ShouldFail)
{
    public static ProducerBackpressureDecision Block() => new(true, false, false);
    public static ProducerBackpressureDecision Drop() => new(false, true, false);
    public static ProducerBackpressureDecision Fail() => new(false, false, true);
}

public interface IProducerBackpressurePolicy
{
    ProducerBackpressureDecision Evaluate(ProducerBackpressureContext context);
}
