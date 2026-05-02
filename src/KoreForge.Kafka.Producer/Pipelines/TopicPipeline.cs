using System.Collections.Generic;
using System.Threading.Channels;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Buffer;
using KoreForge.Kafka.Producer.Workers;

namespace KoreForge.Kafka.Producer.Pipelines;

internal sealed class TopicPipeline
{
    public TopicPipeline(ProducerTopicDefinition definition)
    {
        Definition = definition;
        Buffer = new TopicBufferState(definition.ChannelCapacity);
        Channel = System.Threading.Channels.Channel.CreateBounded<OutgoingEnvelope>(new BoundedChannelOptions(definition.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
        Workers = new List<ProducerWorkerHandle>(definition.WorkerCount);
    }

    public ProducerTopicDefinition Definition { get; }
    public Channel<OutgoingEnvelope> Channel { get; }
    public TopicBufferState Buffer { get; }
    public List<ProducerWorkerHandle> Workers { get; }
}
