using System.Collections.Generic;
using System.Threading.Channels;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Buffer;
using KF.Kafka.Producer.Workers;

namespace KF.Kafka.Producer.Pipelines;

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
