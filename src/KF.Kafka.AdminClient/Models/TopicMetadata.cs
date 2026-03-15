namespace KF.Kafka.AdminClient.Models;

public sealed record TopicMetadata
{
    public string TopicName { get; init; } = string.Empty;
    public bool Exists { get; init; }
    public int PartitionCount { get; init; }
    public int ReplicationFactor { get; init; }
}
