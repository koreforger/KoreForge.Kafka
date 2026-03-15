namespace KF.Kafka.Consumer.Pipelines;

/// <summary>
/// Reserved keys that the consumer adds to the <see cref="KoreForge.Processing.Pipelines.PipelineContext"/>.
/// </summary>
public static class KafkaPipelineContextKeys
{
    /// <summary>
    /// Reference to the <see cref="Batch.KafkaRecordBatch"/> being processed.
    /// </summary>
    public const string Batch = "kafka.consumer.batch";
}
