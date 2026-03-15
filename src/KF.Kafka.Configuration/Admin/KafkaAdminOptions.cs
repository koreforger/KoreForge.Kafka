using System.ComponentModel.DataAnnotations;

namespace KF.Kafka.Configuration.Admin;

/// <summary>
/// Strongly typed settings for the Kafka admin client.
/// </summary>
public sealed class KafkaAdminOptions
{
    /// <summary>
    /// Bootstrap servers that the admin client should use.
    /// </summary>
    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Optional logical client identifier. Defaults to a deterministic "khaos-admin" prefix.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// Timeout (milliseconds) applied to broker interactions.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int RequestTimeoutMs { get; set; } = 10_000;

    /// <summary>
    /// Maximum number of concurrent broker calls when fan-out is required.
    /// </summary>
    [Range(1, 128)]
    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Optional prefix used when the admin client opens short-lived consumers for metadata queries.
    /// </summary>
    public string InternalConsumerGroupPrefix { get; set; } = "khaos-admin";

    public KafkaAdminRetryOptions Retry { get; init; } = new();

    public KafkaAdminCacheOptions Cache { get; init; } = new();

    public KafkaAdminLagHealthOptions LagHealth { get; init; } = new();

    public KafkaAdminSecurityOptions Security { get; init; } = new();
}

public sealed class KafkaAdminRetryOptions
{
    [Range(0, 10)]
    public int MaxRetries { get; set; } = 3;

    [Range(1, 120_000)]
    public int InitialBackoffMs { get; set; } = 200;

    [Range(1, 300_000)]
    public int MaxBackoffMs { get; set; } = 2_000;
}

public sealed class KafkaAdminCacheOptions
{
    public bool LagCacheEnabled { get; set; } = true;
    public bool MetadataCacheEnabled { get; set; } = true;

    [Range(1, 3_600_000)]
    public int LagCacheTtlMs { get; set; } = 1_000;

    [Range(1, 3_600_000)]
    public int MetadataCacheTtlMs { get; set; } = 30_000;
}

public sealed class KafkaAdminLagHealthOptions
{
    /// <summary>
    /// Total lag under this threshold is considered healthy.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long HealthyTotalLagThreshold { get; set; } = 10_000;

    /// <summary>
    /// Max per-partition lag tolerated before we flag an unhealthy summary.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long WarningPartitionLagThreshold { get; set; } = 50_000;
}

public sealed class KafkaAdminSecurityOptions
{
    /// <summary>
    /// Security protocol (Plaintext, Ssl, SaslPlaintext, SaslSsl).
    /// </summary>
    public string? SecurityProtocol { get; set; }

    /// <summary>
    /// SASL mechanism when SASL security is used (e.g., PLAIN, SCRAM-SHA-512).
    /// </summary>
    public string? SaslMechanism { get; set; }

    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }

    /// <summary>
    /// Optional CA certificate location for SSL connections.
    /// </summary>
    public string? SslCaLocation { get; set; }
}
