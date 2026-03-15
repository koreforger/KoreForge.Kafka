using System;
using System.Collections.Generic;

namespace KF.Kafka.Consumer.Routing;

/// <summary>
/// Extracts a routing key and typed message from raw Kafka record bytes.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the deserialized message.</typeparam>
public interface IMessageRouter<TKey, TMessage>
{
    /// <summary>
    /// Attempts to parse a raw Kafka message and extract routing information.
    /// </summary>
    /// <param name="key">Raw Kafka key bytes (may be empty).</param>
    /// <param name="value">Raw Kafka value bytes.</param>
    /// <param name="headers">Kafka headers (may be null).</param>
    /// <param name="result">The parsed message with routing key if successful.</param>
    /// <returns>True if successfully parsed, false to discard the message.</returns>
    bool TryRoute(
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        IReadOnlyDictionary<string, byte[]>? headers,
        out RoutedMessage<TKey, TMessage> result);
}
