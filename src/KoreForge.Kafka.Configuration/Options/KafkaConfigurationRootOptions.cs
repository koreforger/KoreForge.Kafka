using System;
using System.Collections.Generic;
using KoreForge.Kafka.Configuration.Producer;

namespace KoreForge.Kafka.Configuration.Options;

public sealed class KafkaConfigurationRootOptions
{
    public KafkaDefaultsOptions Defaults { get; init; } = new();

    public Dictionary<string, KafkaClusterSettings> Clusters { get; init; }
        = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, KafkaSecuritySettings> SecurityProfiles { get; init; }
        = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, KafkaProfileSettings> Profiles { get; init; }
        = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class KafkaDefaultsOptions
{
    public string GroupIdTemplate { get; init; } = "dev-{AppName}-{ProfileName}-{Random4}";
}

public sealed class KafkaClusterSettings
{
    public string BootstrapServers { get; init; } = string.Empty;

    public Dictionary<string, string> ConfluentOptions { get; init; } = new(StringComparer.OrdinalIgnoreCase);

    public string? DefaultSecurityProfile { get; init; }
}

public enum KafkaSecurityMode
{
    None,
    Certificates
}

public sealed class KafkaSecuritySettings
{
    public KafkaSecurityMode Mode { get; init; } = KafkaSecurityMode.None;

    public string? RootCaBase64 { get; init; }
    public string? ClientCertBase64 { get; init; }
    public string? ClientKeyBase64 { get; init; }
    public string? ClientKeyPassword { get; init; }
}

public enum StartMode
{
    Earliest,
    Latest,
    RelativeTime
}

public sealed class ExtendedConsumerSettings
{
    public int ConsumerCount { get; init; } = 1;
    public StartMode StartMode { get; init; } = StartMode.Earliest;
    public TimeSpan? StartRelative { get; init; }
    public int MaxBatchSize { get; init; } = 500;
    public int MaxBatchWaitMs { get; init; } = 1_000;

    /// <summary>
    /// How long the Confluent consumer poll call blocks waiting for a message.
    /// Lower values reduce latency when the topic is idle; higher values reduce
    /// CPU busy-waiting. Default: 100 ms.
    /// </summary>
    public int PollTimeoutMs { get; init; } = 100;

    /// <summary>
    /// How long the Confluent consumer poll call blocks while the consumer is paused for backpressure.
    /// This must still poll periodically so heartbeats, callbacks, commits, and rebalances are serviced.
    /// Default: 1000 ms.
    /// </summary>
    public int PausedPollTimeoutMs { get; init; } = 1_000;

    /// <summary>
    /// Maximum degree of parallelism for processing records within a single batch.
    /// 1 (default) = sequential. Set higher to process records in a batch concurrently
    /// when pipeline steps are thread-safe and I/O-bound.
    /// </summary>
    public int PipelineMaxDegreeOfParallelism { get; init; } = 1;

    public bool IsolateConsumerCertificatePerThread { get; init; }
    public bool DuplicateCertificatePerWorker { get; init; }
    public int MaxInFlightBatches { get; init; } = 8;
    public int BatchProcessingTimeoutMs { get; init; } = 300_000;
    public ConsumerBatchTimeoutAction BatchTimeoutAction { get; init; } = ConsumerBatchTimeoutAction.FailWorker;
    public int BackpressureSummaryLogIntervalMs { get; init; } = 30_000;
    public ConsumerBackpressureSettings Backpressure { get; init; } = new();
}

public enum ConsumerBatchTimeoutAction
{
    FailWorker,
    CancelBatchAndFailWorker
}

public sealed class ConsumerBackpressureSettings
{
    public bool Coordinated { get; init; } = true;
    public double PauseUtilization { get; init; } = 1.0;
    public double ResumeUtilization { get; init; } = 0.5;
}

public sealed class ExtendedProducerSettings
{
    public ProducerTopicDefaults TopicDefaults { get; init; } = new();
    public Dictionary<string, ProducerTopicSettings> Topics { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public ProducerBackpressureSettings Backpressure { get; init; } = new();
    public ProducerRestartSettings Restart { get; init; } = new();
    public ProducerBacklogSettings Backlog { get; init; } = new();
    public ProducerOperationalSettings Operations { get; init; } = new();
    public ProducerAlertSettings Alerts { get; init; } = new();
}

public sealed class ExtendedAdminSettings
{
}

public enum KafkaClientType
{
    Consumer,
    Producer,
    Admin
}

public sealed class KafkaProfileSettings
{
    public KafkaClientType Type { get; init; }
    public string Cluster { get; init; } = string.Empty;
    public string? SecurityProfile { get; init; }
    public string? ExplicitGroupId { get; init; }
    public string? GroupIdTemplate { get; init; }
    public List<string> Topics { get; init; } = new();
    public Dictionary<string, string> ConfluentOptions { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public ExtendedConsumerSettings ExtendedConsumer { get; set; } = new();
    public ExtendedProducerSettings ExtendedProducer { get; set; } = new();
    public ExtendedAdminSettings ExtendedAdmin { get; set; } = new();

    public KafkaProfileExtendedSettings? Extended
    {
        get => null;
        init
        {
            if (value?.Consumer is not null)
            {
                ExtendedConsumer = value.Consumer;
            }

            if (value?.Producer is not null)
            {
                ExtendedProducer = value.Producer;
            }

            if (value?.Admin is not null)
            {
                ExtendedAdmin = value.Admin;
            }
        }
    }
}

public sealed class KafkaProfileExtendedSettings
{
    public ExtendedConsumerSettings? Consumer { get; init; }
    public ExtendedProducerSettings? Producer { get; init; }
    public ExtendedAdminSettings? Admin { get; init; }
}
