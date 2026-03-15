using System.Collections.Generic;

namespace KF.Kafka.Producer.Abstractions;

public sealed class ProducerEnqueueOptions
{
    public string? TopicOverride { get; init; }
    public string? Key { get; init; }
    public IDictionary<string, string>? Headers { get; init; }
}
