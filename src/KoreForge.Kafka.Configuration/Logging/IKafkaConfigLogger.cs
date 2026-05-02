using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;

namespace KoreForge.Kafka.Configuration.Logging;

public interface IKafkaConfigLogger
{
    string ToLogString(ClientConfig config);
    IReadOnlyDictionary<string, object?> ToLogProperties(ClientConfig config);
}

public sealed class DefaultKafkaConfigLogger : IKafkaConfigLogger
{
    private static readonly HashSet<string> SensitiveKeys = new(StringComparer.OrdinalIgnoreCase)
    {
        "sasl.password",
        "ssl.key.password",
        "ssl.keystore.password",
        "ssl.key.pem",
        "ssl.key",
        "sasl.jaas.config"
    };

    public string ToLogString(ClientConfig config)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));

        var builder = new StringBuilder();
        var first = true;
        foreach (var pair in config)
        {
            if (!first)
            {
                builder.Append(' ');
            }

            builder.Append(pair.Key);
            builder.Append('=');
            builder.Append(MaskIfSensitive(pair.Key, pair.Value));
            first = false;
        }

        return builder.ToString();
    }

    public IReadOnlyDictionary<string, object?> ToLogProperties(ClientConfig config)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));

        var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in config)
        {
            var key = $"kafka.{pair.Key}";
            dict[key] = MaskIfSensitive(pair.Key, pair.Value);
        }

        return dict;
    }

    private static string MaskIfSensitive(string key, string value)
    {
        if (SensitiveKeys.Contains(key))
        {
            return "***MASKED***";
        }

        return value;
    }
}
