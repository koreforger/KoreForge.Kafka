using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace KF.Kafka.Consumer.Abstractions;

public sealed record KafkaConsumerLifecycleEvent(
    int WorkerId,
    IReadOnlyList<TopicPartition> Partitions,
    DateTimeOffset OccurredAtUtc);

public interface IKafkaConsumerLifecycleObserver
{
    void OnPartitionsAssigned(KafkaConsumerLifecycleEvent lifecycleEvent);

    void OnPartitionsRevoked(KafkaConsumerLifecycleEvent lifecycleEvent);
}
