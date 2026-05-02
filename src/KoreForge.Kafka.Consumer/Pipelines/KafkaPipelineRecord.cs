using Confluent.Kafka;

namespace KoreForge.Kafka.Consumer.Pipelines;

/// <summary>
/// Lightweight projection of a Kafka <see cref="ConsumeResult{TKey, TValue}"/> exposed to processing pipelines.
/// </summary>
public readonly record struct KafkaPipelineRecord
{
    public KafkaPipelineRecord(ConsumeResult<byte[], byte[]> source)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        Topic = source.Topic;
        Partition = source.Partition.Value;
        Offset = source.Offset.Value;
        Timestamp = source.Message?.Timestamp ?? Timestamp.Default;
        Headers = source.Message?.Headers ?? new Headers();
        Key = source.Message?.Key is { } key ? key.AsMemory() : null;
        Value = source.Message?.Value is { } value ? value.AsMemory() : ReadOnlyMemory<byte>.Empty;
    }

    /// <summary>
    /// Underlying Confluent record for callers that need advanced metadata.
    /// </summary>
    public ConsumeResult<byte[], byte[]> Source { get; }

    public string Topic { get; }

    public int Partition { get; }

    public long Offset { get; }

    public Timestamp Timestamp { get; }

    public Headers Headers { get; }

    public ReadOnlyMemory<byte>? Key { get; }

    public ReadOnlyMemory<byte> Value { get; }

    public bool HasKey => Key.HasValue;

    public static KafkaPipelineRecord FromConsumeResult(ConsumeResult<byte[], byte[]> record) => new(record);
}
