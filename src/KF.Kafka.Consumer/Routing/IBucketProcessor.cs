using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KF.Kafka.Consumer.Routing;

/// <summary>
/// Processes a batch of messages routed to a single bucket.
/// Called from a single worker thread per bucket (single-writer guarantee).
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the deserialized message.</typeparam>
public interface IBucketProcessor<TKey, TMessage>
{
    /// <summary>
    /// Processes a batch of messages for this bucket.
    /// All messages in the batch have routing keys that map to this bucket.
    /// </summary>
    /// <param name="bucketIndex">Index of the bucket (0 to BucketCount-1).</param>
    /// <param name="messages">Batch of messages to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ProcessBatchAsync(
        int bucketIndex,
        IReadOnlyList<RoutedMessage<TKey, TMessage>> messages,
        CancellationToken cancellationToken);
}
