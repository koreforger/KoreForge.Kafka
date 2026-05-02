using System;
using System.Collections.Generic;

namespace KoreForge.Kafka.Configuration.Producer;

public enum ProducerBackpressureMode
{
    Block,
    Drop,
    Fail
}

public sealed class ProducerTopicDefaults
{
    public int WorkerCount { get; init; } = 1;
    public int ChannelCapacity { get; init; } = 10_000;
    public ProducerBackpressureMode BackpressureMode { get; init; } = ProducerBackpressureMode.Block;
}

public sealed class ProducerTopicSettings
{
    public int? WorkerCount { get; init; }
    public int? ChannelCapacity { get; init; }
    public ProducerBackpressureMode? BackpressureMode { get; init; }
    public ProducerBacklogSettings? Backlog { get; init; }
}

public sealed class ProducerBackpressureSettings
{
    public ProducerBackpressureMode Mode { get; init; } = ProducerBackpressureMode.Block;
    public TimeSpan BlockTimeout { get; init; } = TimeSpan.FromSeconds(15);
    public bool ThrowOnTimeout { get; init; } = true;
}

public sealed class ProducerBacklogSettings
{
    public bool PersistenceEnabled { get; init; }
    public bool ReplayEnabled { get; init; }
    public string Directory { get; init; } = "producer-backlog";
    public string? FileCompression { get; init; }
    public bool ArchiveOnRestore { get; init; }
    public string Strategy { get; init; } = ProducerBacklogStrategies.File;
}

public static class ProducerBacklogStrategies
{
    public const string File = "file";
    public const string None = "none";
}

public sealed class ProducerRestartSettings
{
    public TimeSpan BackoffInitial { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan BackoffMax { get; init; } = TimeSpan.FromSeconds(30);
    public double BackoffMultiplier { get; init; } = 2.0;
    public TimeSpan FailureWindow { get; init; } = TimeSpan.FromMinutes(1);
    public int MaxAttemptsInWindow { get; init; } = 5;
    public TimeSpan LongTermRetryInterval { get; init; } = TimeSpan.FromMinutes(15);
}

public sealed class ProducerOperationalSettings
{
    public int MaxBatchSize { get; init; } = 100;
    public int MaxBatchWindowMs { get; init; } = 5;
}

public sealed class ProducerAlertSettings
{
    public bool Enabled { get; init; } = true;
    public TimeSpan EvaluationInterval { get; init; } = TimeSpan.FromSeconds(5);
}
