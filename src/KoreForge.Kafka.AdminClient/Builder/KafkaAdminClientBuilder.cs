using Confluent.Kafka;
using KoreForge.Kafka.AdminClient.Abstractions;
using KoreForge.Kafka.AdminClient.Instrumentation;
using KoreForge.Kafka.Configuration.Admin;
using KoreForge.Kafka.Configuration.Factory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KoreForge.Kafka.AdminClient.Builder;

public sealed class KafkaAdminClientBuilder
{
    private readonly KafkaAdminOptions _options = new();
    private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
    private IKafkaAdminMetrics _metrics = NoOpKafkaAdminMetrics.Instance;
    private AdminClientConfig? _adminClientConfig;

    public KafkaAdminClientBuilder UseOptions(Action<KafkaAdminOptions> configure)
    {
        configure?.Invoke(_options);
        return this;
    }

    public KafkaAdminClientBuilder UseLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        return this;
    }

    public KafkaAdminClientBuilder UseMetrics(IKafkaAdminMetrics metrics)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        return this;
    }

    public KafkaAdminClientBuilder UseKafkaConfigurationProfile(string profileName, IKafkaClientConfigFactory factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _ = factory ?? throw new ArgumentNullException(nameof(factory));

        var runtime = factory.CreateAdmin(profileName);
        _adminClientConfig = new AdminClientConfig(runtime.ConfluentConfig);
        ApplyAdminConfigToOptions(_adminClientConfig);

        return this;
    }

    public IKafkaAdminClient Build()
    {
        var logger = _loggerFactory.CreateLogger<Internal.KafkaAdminClient>();
        return new Internal.KafkaAdminClient(Options.Create(_options), logger, _metrics, adminClientConfig: _adminClientConfig);
    }

    private void ApplyAdminConfigToOptions(AdminClientConfig config)
    {
        if (!string.IsNullOrWhiteSpace(config.BootstrapServers))
        {
            _options.BootstrapServers = config.BootstrapServers!;
        }

        if (!string.IsNullOrWhiteSpace(config.ClientId))
        {
            _options.ClientId = config.ClientId;
        }

        if (config.SocketTimeoutMs.HasValue)
        {
            _options.RequestTimeoutMs = config.SocketTimeoutMs.Value;
        }

        if (config.SecurityProtocol.HasValue)
        {
            _options.Security.SecurityProtocol = config.SecurityProtocol.Value.ToString();
        }

        if (config.SaslMechanism.HasValue)
        {
            _options.Security.SaslMechanism = config.SaslMechanism.Value.ToString();
        }

        _options.Security.SaslUsername = PreferOverride(config.SaslUsername, _options.Security.SaslUsername);
        _options.Security.SaslPassword = PreferOverride(config.SaslPassword, _options.Security.SaslPassword);
        _options.Security.SslCaLocation = PreferOverride(config.SslCaLocation, _options.Security.SslCaLocation);
    }

    private static string? PreferOverride(string? candidate, string? existing) =>
        string.IsNullOrWhiteSpace(candidate) ? existing : candidate;
}

public static class KafkaAdminClientHost
{
    public static KafkaAdminClientBuilder Create() => new();
}
