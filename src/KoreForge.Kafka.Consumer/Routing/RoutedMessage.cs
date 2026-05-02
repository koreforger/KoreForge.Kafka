using System;
using System.Collections.Generic;

namespace KoreForge.Kafka.Consumer.Routing;

/// <summary>
/// Message envelope with routing information extracted from a Kafka record.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the deserialized message.</typeparam>
public readonly struct RoutedMessage<TKey, TMessage>
{
    /// <summary>
    /// The key used for bucket routing.
    /// </summary>
    public required TKey RoutingKey { get; init; }

    /// <summary>
    /// The deserialized message payload.
    /// </summary>
    public required TMessage Message { get; init; }

    /// <summary>
    /// The Kafka partition this message was consumed from.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// The offset within the partition.
    /// </summary>
    public long Offset { get; init; }

    /// <summary>
    /// The timestamp associated with the message.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Optional headers from the Kafka message.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }
}
