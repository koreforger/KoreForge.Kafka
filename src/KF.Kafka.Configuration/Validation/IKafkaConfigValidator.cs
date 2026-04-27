using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Configuration.Runtime;

namespace KF.Kafka.Configuration.Validation;

public sealed class KafkaConfigValidationResult
{
    public bool IsValid => Errors.Count == 0;
    public List<string> Errors { get; } = new();
    public List<string> Warnings { get; } = new();
}

public interface IKafkaConfigValidator
{
    KafkaConfigValidationResult ValidateConsumer(KafkaConsumerRuntimeConfig config);
    KafkaConfigValidationResult ValidateProducer(KafkaProducerRuntimeConfig config);
    KafkaConfigValidationResult ValidateAdmin(KafkaAdminRuntimeConfig config);
}

public sealed class KafkaConfigValidator : IKafkaConfigValidator
{
    public KafkaConfigValidationResult ValidateConsumer(KafkaConsumerRuntimeConfig config)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));

        var result = ValidateCommon(config.ConfluentConfig);

        if (string.IsNullOrWhiteSpace(config.ConfluentConfig.GroupId))
        {
            result.Errors.Add("GroupId must be provided for consumer profiles.");
        }

        if (config.Topics.Count == 0)
        {
            result.Errors.Add("At least one topic must be configured for consumer profiles.");
        }

        if (config.Extended.ConsumerCount < 1)
        {
            result.Errors.Add("Extended.ConsumerCount must be at least 1.");
        }

        if (config.Extended.MaxBatchSize < 1)
        {
            result.Errors.Add("Extended.MaxBatchSize must be at least 1.");
        }

        if (config.Extended.MaxBatchWaitMs < 1)
        {
            result.Errors.Add("Extended.MaxBatchWaitMs must be at least 1.");
        }

        if (config.Extended.PollTimeoutMs < 1)
        {
            result.Errors.Add("Extended.PollTimeoutMs must be at least 1.");
        }

        if (config.Extended.PausedPollTimeoutMs < 1)
        {
            result.Errors.Add("Extended.PausedPollTimeoutMs must be at least 1.");
        }

        if (config.Extended.MaxInFlightBatches < 1)
        {
            result.Errors.Add("Extended.MaxInFlightBatches must be at least 1.");
        }

        if (config.Extended.BatchProcessingTimeoutMs < 1)
        {
            result.Errors.Add("Extended.BatchProcessingTimeoutMs must be at least 1.");
        }

        if (config.Extended.BackpressureSummaryLogIntervalMs < 0)
        {
            result.Errors.Add("Extended.BackpressureSummaryLogIntervalMs cannot be negative.");
        }

        if (config.Extended.DuplicateCertificatePerWorker && !config.Extended.IsolateConsumerCertificatePerThread)
        {
            result.Errors.Add("Extended.DuplicateCertificatePerWorker requires Extended.IsolateConsumerCertificatePerThread to be true.");
        }

        if (config.Extended.StartMode == StartMode.RelativeTime)
        {
            if (config.Extended.StartRelative is null || config.Extended.StartRelative <= TimeSpan.Zero)
            {
                result.Errors.Add("Extended.StartRelative must be greater than zero when StartMode is RelativeTime.");
            }
        }

        return result;
    }

    public KafkaConfigValidationResult ValidateProducer(KafkaProducerRuntimeConfig config)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));

        var result = ValidateCommon(config.ConfluentConfig);

        if (config.Topics.Count == 0)
        {
            result.Errors.Add("At least one topic must be configured for producer profiles.");
        }

        var defaults = config.Extended.TopicDefaults ?? new ProducerTopicDefaults();
        if (defaults.WorkerCount < 1)
        {
            result.Errors.Add("Extended.TopicDefaults.WorkerCount must be at least 1.");
        }

        if (defaults.ChannelCapacity < 1)
        {
            result.Errors.Add("Extended.TopicDefaults.ChannelCapacity must be at least 1.");
        }

        foreach (var topic in config.Topics)
        {
            var overrides = config.Extended.Topics.TryGetValue(topic, out var settings) ? settings : null;
            var workerCount = overrides?.WorkerCount ?? defaults.WorkerCount;
            var capacity = overrides?.ChannelCapacity ?? defaults.ChannelCapacity;

            if (workerCount < 1)
            {
                result.Errors.Add($"Topic '{topic}' worker count must be at least 1.");
            }

            if (capacity < 1)
            {
                result.Errors.Add($"Topic '{topic}' channel capacity must be at least 1.");
            }
        }

        if (config.Extended.Backpressure.BlockTimeout < TimeSpan.Zero)
        {
            result.Errors.Add("Extended.Backpressure.BlockTimeout cannot be negative.");
        }

        if (config.Extended.Restart.BackoffInitial <= TimeSpan.Zero)
        {
            result.Errors.Add("Extended.Restart.BackoffInitial must be greater than zero.");
        }

        if (config.Extended.Restart.BackoffMax < config.Extended.Restart.BackoffInitial)
        {
            result.Errors.Add("Extended.Restart.BackoffMax must be greater than or equal to BackoffInitial.");
        }

        if (config.Extended.Restart.BackoffMultiplier < 1)
        {
            result.Errors.Add("Extended.Restart.BackoffMultiplier must be at least 1.");
        }

        if (config.Extended.Restart.MaxAttemptsInWindow < 1)
        {
            result.Errors.Add("Extended.Restart.MaxAttemptsInWindow must be at least 1.");
        }

        if (config.Extended.Restart.FailureWindow <= TimeSpan.Zero)
        {
            result.Errors.Add("Extended.Restart.FailureWindow must be greater than zero.");
        }

        if (config.Extended.Restart.LongTermRetryInterval <= TimeSpan.Zero)
        {
            result.Errors.Add("Extended.Restart.LongTermRetryInterval must be greater than zero.");
        }

        if (config.Extended.Backlog.PersistenceEnabled && string.IsNullOrWhiteSpace(config.Extended.Backlog.Directory))
        {
            result.Errors.Add("Extended.Backlog.Directory must be provided when persistence is enabled.");
        }

        if (config.Extended.Operations.MaxBatchSize < 1)
        {
            result.Errors.Add("Extended.Operations.MaxBatchSize must be at least 1.");
        }

        if (config.Extended.Operations.MaxBatchWindowMs < 0)
        {
            result.Errors.Add("Extended.Operations.MaxBatchWindowMs cannot be negative.");
        }

        return result;
    }

    public KafkaConfigValidationResult ValidateAdmin(KafkaAdminRuntimeConfig config)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));

        return ValidateCommon(config.ConfluentConfig);
    }

    private static KafkaConfigValidationResult ValidateCommon(ClientConfig config)
    {
        var result = new KafkaConfigValidationResult();

        if (string.IsNullOrWhiteSpace(config.BootstrapServers))
        {
            result.Errors.Add("BootstrapServers is required.");
        }

        if (config.SecurityProtocol is SecurityProtocol.Ssl or SecurityProtocol.SaslSsl)
        {
            if (string.IsNullOrWhiteSpace(config.SslCaLocation))
            {
                result.Errors.Add("SslCaLocation must be provided for SSL-enabled profiles.");
            }
            else if (!File.Exists(config.SslCaLocation))
            {
                result.Errors.Add($"SslCaLocation '{config.SslCaLocation}' does not exist.");
            }
        }

        return result;
    }
}
