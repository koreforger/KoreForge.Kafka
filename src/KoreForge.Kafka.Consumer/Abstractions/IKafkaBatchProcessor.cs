using System.Threading;
using System.Threading.Tasks;
using KoreForge.Kafka.Consumer.Batch;

namespace KoreForge.Kafka.Consumer.Abstractions;

/// <summary>
/// Application-supplied batch processor invoked by each consumer worker.
/// </summary>
public interface IKafkaBatchProcessor
{
    /// <summary>
    /// Processes the supplied batch. Implementations must be thread-safe only if shared across workers.
    /// </summary>
    Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken);
}
