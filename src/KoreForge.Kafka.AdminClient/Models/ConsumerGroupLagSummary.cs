namespace KoreForge.Kafka.AdminClient.Models;

public sealed record ConsumerGroupLagSummary
{
    public string ConsumerGroupId { get; init; } = string.Empty;
    public long TotalLag { get; init; }
    public long MaxLagPerPartition { get; init; }
    public int PartitionsWithLag { get; init; }
    public int TotalPartitions { get; init; }
    public bool IsHealthy { get; init; }
}
