using Confluent.Kafka;
using KoreForge.Kafka.AdminClient.Models;
using TopicMetadataModel = KoreForge.Kafka.AdminClient.Models.TopicMetadata;

namespace KoreForge.Kafka.AdminClient.Abstractions;

/// <summary>
/// High-level read-only Kafka administration surface used by consumer and producer libraries.
/// </summary>
public interface IKafkaAdminClient : IAsyncDisposable
{
    /// <summary>
    /// Retrieves topic metadata (existence, partition count, replication factor).
    /// </summary>
    Task<TopicMetadataModel> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Computes lag per topic/partition for a given consumer group.
    /// </summary>
    Task<IReadOnlyList<ConsumerGroupLag>> GetConsumerGroupLagAsync(
        string consumerGroupId,
        ConsumerGroupLagQuery query,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Aggregates lag metrics into a summary with health flags.
    /// </summary>
    Task<ConsumerGroupLagSummary> GetConsumerGroupLagSummaryAsync(
        string consumerGroupId,
        ConsumerGroupLagQuery query,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves offsets nearest to the supplied UTC timestamp for every partition in <paramref name="topicName"/>.
    /// </summary>
    Task<IReadOnlyDictionary<TopicPartition, long>> GetOffsetsForTimestampAsync(
        string topicName,
        DateTime timestampUtc,
        CancellationToken cancellationToken = default);
}
