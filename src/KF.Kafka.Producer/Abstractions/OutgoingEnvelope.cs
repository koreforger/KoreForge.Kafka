using System;
using System.Collections.Generic;

namespace KF.Kafka.Producer.Abstractions;

public sealed class OutgoingEnvelope
{
    public OutgoingEnvelope(
        object payload,
        Type payloadType,
        string topic,
        string? key,
        IDictionary<string, string>? headers,
        DateTimeOffset localCreatedAt)
    {
        Payload = payload ?? throw new ArgumentNullException(nameof(payload));
        PayloadType = payloadType ?? throw new ArgumentNullException(nameof(payloadType));
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        Key = key;
        Headers = headers;
        LocalCreatedAt = localCreatedAt;
    }

    public object Payload { get; }
    public Type PayloadType { get; }
    public string Topic { get; }
    public string? Key { get; }
    public IDictionary<string, string>? Headers { get; }
    public DateTimeOffset LocalCreatedAt { get; }
}
