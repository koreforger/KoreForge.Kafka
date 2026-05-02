using System.Collections.Generic;
using KoreForge.Kafka.Producer.Runtime;

namespace KoreForge.Kafka.Producer.Backlog;

internal interface IProducerBacklogDiagnostics
{
    ProducerBacklogTopicStatus? GetTopicStatus(string topicName);
    IReadOnlyDictionary<string, ProducerBacklogTopicStatus> GetAllTopics();
}
