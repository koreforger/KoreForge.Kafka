using KoreForge.Kafka.Configuration.Producer;

namespace KoreForge.Kafka.Producer.Pipelines;

internal sealed class ProducerTopicDefinition
{
    public ProducerTopicDefinition(
        string topicName,
        int workerCount,
        int channelCapacity,
        ProducerBackpressureMode backpressureMode,
        ProducerBacklogSettings backlogSettings)
    {
        TopicName = topicName;
        WorkerCount = workerCount;
        ChannelCapacity = channelCapacity;
        BackpressureMode = backpressureMode;
        BacklogSettings = backlogSettings;
    }

    public string TopicName { get; }
    public int WorkerCount { get; }
    public int ChannelCapacity { get; }
    public ProducerBackpressureMode BackpressureMode { get; }
    public ProducerBacklogSettings BacklogSettings { get; }
}
