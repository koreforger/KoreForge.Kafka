using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using KoreForge.Kafka.Producer.Abstractions;

namespace KoreForge.Kafka.Producer.Serialization;

public sealed class JsonProducerSerializer<TMessage> : IProducerSerializer<TMessage>
{
    private readonly JsonSerializerOptions _options;

    public JsonProducerSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public SerializedRecord Serialize(TMessage message, OutgoingEnvelope envelope)
    {
        var valueBytes = JsonSerializer.SerializeToUtf8Bytes(message, _options);
        byte[]? keyBytes = null;
        if (!string.IsNullOrEmpty(envelope.Key))
        {
            keyBytes = Encoding.UTF8.GetBytes(envelope.Key!);
        }

        IReadOnlyDictionary<string, byte[]>? headers = null;
        if (envelope.Headers is { Count: > 0 })
        {
            var converted = new Dictionary<string, byte[]>(envelope.Headers.Count, System.StringComparer.Ordinal);
            foreach (var kvp in envelope.Headers)
            {
                converted[kvp.Key] = Encoding.UTF8.GetBytes(kvp.Value);
            }

            headers = converted;
        }

        return new SerializedRecord
        {
            Topic = envelope.Topic,
            KeyBytes = keyBytes,
            ValueBytes = valueBytes,
            Headers = headers
        };
    }
}
