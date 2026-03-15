using System.Collections.Generic;
using KF.Kafka.Producer.Runtime;

namespace KF.Kafka.Producer.Backlog;

internal interface IProducerBacklogDiagnostics
{
    ProducerBacklogTopicStatus? GetTopicStatus(string topicName);
    IReadOnlyDictionary<string, ProducerBacklogTopicStatus> GetAllTopics();
}
