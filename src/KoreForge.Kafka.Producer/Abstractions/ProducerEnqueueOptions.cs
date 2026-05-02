using System.Collections.Generic;

namespace KoreForge.Kafka.Producer.Abstractions;

public sealed class ProducerEnqueueOptions
{
    public string? TopicOverride { get; init; }
    public string? Key { get; init; }
    public IDictionary<string, string>? Headers { get; init; }
}
