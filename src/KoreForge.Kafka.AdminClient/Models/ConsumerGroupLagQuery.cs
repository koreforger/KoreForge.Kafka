namespace KoreForge.Kafka.AdminClient.Models;

public sealed record ConsumerGroupLagQuery
{
    public IReadOnlyList<string>? TopicFilter { get; init; }
    public bool ExcludeEmptyTopics { get; init; }
}
