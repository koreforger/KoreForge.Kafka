using System;
using System.Collections.Generic;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Configuration.Runtime;

namespace KF.Kafka.Producer.Pipelines;

internal static class ProducerTopologyBuilder
{
    public static IReadOnlyList<ProducerTopicDefinition> BuildTopics(KafkaProducerRuntimeConfig config)
    {
        var topics = new List<ProducerTopicDefinition>(config.Topics.Count);
        var defaults = config.Extended.TopicDefaults ?? new ProducerTopicDefaults();
        foreach (var topicName in config.Topics)
        {
            var overrides = config.Extended.Topics.TryGetValue(topicName, out var topicSettings)
                ? topicSettings
                : null;

            var workerCount = overrides?.WorkerCount ?? defaults.WorkerCount;
            var capacity = overrides?.ChannelCapacity ?? defaults.ChannelCapacity;
            var mode = overrides?.BackpressureMode ?? defaults.BackpressureMode;
            var backlog = overrides?.Backlog ?? config.Extended.Backlog ?? new ProducerBacklogSettings();

            topics.Add(new ProducerTopicDefinition(topicName, workerCount, capacity, mode, backlog));
        }

        if (topics.Count == 0)
        {
            throw new InvalidOperationException("Producer profile did not define any topics.");
        }

        return topics;
    }
}
