using Confluent.Kafka;
using KoreForge.Kafka.Configuration.Admin;

namespace KoreForge.Kafka.AdminClient.Internal;

internal interface IKafkaConsumerFactory
{
    IConsumer<byte[], byte[]> CreateConsumer();
}

internal sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly KafkaAdminOptions _options;

    public KafkaConsumerFactory(KafkaAdminOptions options)
    {
        _options = options;
    }

    public IConsumer<byte[], byte[]> CreateConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = $"{_options.InternalConsumerGroupPrefix}-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false,
            AutoOffsetReset = AutoOffsetReset.Latest,
            SocketTimeoutMs = _options.RequestTimeoutMs,
            SessionTimeoutMs = Math.Max(3_000, _options.RequestTimeoutMs)
        };

        ApplySecurity(config, _options.Security);

        return new ConsumerBuilder<byte[], byte[]>(config).Build();
    }

    internal static void ApplySecurity(ClientConfig config, KafkaAdminSecurityOptions security)
    {
        if (!string.IsNullOrWhiteSpace(security.SecurityProtocol) &&
            Enum.TryParse<SecurityProtocol>(security.SecurityProtocol, true, out var protocol))
        {
            config.SecurityProtocol = protocol;
        }

        if (!string.IsNullOrWhiteSpace(security.SaslMechanism) &&
            Enum.TryParse<SaslMechanism>(security.SaslMechanism, true, out var mechanism))
        {
            config.SaslMechanism = mechanism;
        }

        if (!string.IsNullOrWhiteSpace(security.SaslUsername))
        {
            config.SaslUsername = security.SaslUsername;
        }

        if (!string.IsNullOrWhiteSpace(security.SaslPassword))
        {
            config.SaslPassword = security.SaslPassword;
        }

        if (!string.IsNullOrWhiteSpace(security.SslCaLocation))
        {
            config.SslCaLocation = security.SslCaLocation;
        }
    }
}
