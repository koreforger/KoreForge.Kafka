namespace KoreForge.Kafka.AdminClient.Models;

public sealed record ConsumerGroupLag
{
    public string ConsumerGroupId { get; init; } = string.Empty;
    public string TopicName { get; init; } = string.Empty;
    public int Partition { get; init; }
    public long EndOffset { get; init; }
    public long CommittedOffset { get; init; }
    public DateTime? EndOffsetTimestampUtc { get; init; }

    public long Lag => Math.Max(0, EndOffset - CommittedOffset);
}
