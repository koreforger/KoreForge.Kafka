using System.Threading;
using System.Threading.Tasks;

namespace KF.Kafka.Producer.Abstractions;

public interface IProducerBuffer
{
    ValueTask EnqueueAsync<TMessage>(
        string topicName,
        TMessage message,
        ProducerEnqueueOptions? options = null,
        CancellationToken cancellationToken = default);
}
