using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace KF.Kafka.Producer.Abstractions;

public interface IProducerSerializer<TMessage>
{
    SerializedRecord Serialize(TMessage message, OutgoingEnvelope envelope);
}

public sealed class SerializedRecord
{
    public string Topic { get; init; } = string.Empty;
    public byte[]? KeyBytes { get; init; }
    public byte[] ValueBytes { get; init; } = Array.Empty<byte>();
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }
}

public interface IProducerSerializerRegistry
{
    void RegisterSerializer<TMessage>(IProducerSerializer<TMessage> serializer);
    SerializedRecord Serialize(OutgoingEnvelope envelope);
}

public sealed class ProducerSerializerRegistry : IProducerSerializerRegistry
{
    private readonly ConcurrentDictionary<Type, IEnvelopeSerializer> _serializers = new();

    public void RegisterSerializer<TMessage>(IProducerSerializer<TMessage> serializer)
    {
        if (serializer == null) throw new ArgumentNullException(nameof(serializer));

        var adapter = new EnvelopeSerializer<TMessage>(serializer);
        if (!_serializers.TryAdd(typeof(TMessage), adapter))
        {
            throw new InvalidOperationException($"Serializer already registered for {typeof(TMessage).FullName}.");
        }
    }

    public SerializedRecord Serialize(OutgoingEnvelope envelope)
    {
        if (envelope == null) throw new ArgumentNullException(nameof(envelope));

        if (!_serializers.TryGetValue(envelope.PayloadType, out var serializer))
        {
            throw new InvalidOperationException($"No serializer registered for {envelope.PayloadType.FullName}.");
        }

        return serializer.Serialize(envelope.Payload, envelope);
    }

    private interface IEnvelopeSerializer
    {
        SerializedRecord Serialize(object payload, OutgoingEnvelope envelope);
    }

    private sealed class EnvelopeSerializer<TMessage> : IEnvelopeSerializer
    {
        private readonly IProducerSerializer<TMessage> _serializer;

        public EnvelopeSerializer(IProducerSerializer<TMessage> serializer)
        {
            _serializer = serializer;
        }

        public SerializedRecord Serialize(object payload, OutgoingEnvelope envelope)
        {
            return _serializer.Serialize((TMessage)payload, envelope);
        }
    }
}
