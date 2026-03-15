using System;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Configuration.Validation;
using Xunit;

namespace KF.Kafka.Configuration.Tests;

public class KafkaConfigValidatorTests
{
    [Fact]
    public void ValidateConsumer_Fails_WhenTopicsMissing()
    {
        var validator = new KafkaConfigValidator();
        var runtime = new KafkaConsumerRuntimeConfig(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" },
            new ExtendedConsumerSettings(),
            Array.Empty<string>(),
            new KafkaSecuritySettings());

        var result = validator.ValidateConsumer(runtime);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.Contains("topic", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateConsumer_Fails_WhenMaxBatchSizeInvalid()
    {
        var validator = new KafkaConfigValidator();
        var runtime = new KafkaConsumerRuntimeConfig(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" },
            new ExtendedConsumerSettings { MaxBatchSize = 0 },
            new[] { "topic-1" },
            new KafkaSecuritySettings());

        var result = validator.ValidateConsumer(runtime);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.Contains("MaxBatchSize", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateConsumer_Fails_WhenMaxBatchWaitInvalid()
    {
        var validator = new KafkaConfigValidator();
        var runtime = new KafkaConsumerRuntimeConfig(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" },
            new ExtendedConsumerSettings { MaxBatchWaitMs = 0 },
            new[] { "topic-1" },
            new KafkaSecuritySettings());

        var result = validator.ValidateConsumer(runtime);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.Contains("MaxBatchWaitMs", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateConsumer_Fails_WhenDuplicateWithoutIsolation()
    {
        var validator = new KafkaConfigValidator();
        var runtime = new KafkaConsumerRuntimeConfig(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" },
            new ExtendedConsumerSettings
            {
                DuplicateCertificatePerWorker = true,
                IsolateConsumerCertificatePerThread = false
            },
            new[] { "topic-1" },
            new KafkaSecuritySettings());

        var result = validator.ValidateConsumer(runtime);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.Contains("DuplicateCertificatePerWorker", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ValidateConsumer_Fails_WhenRelativeStartMissing()
    {
        var validator = new KafkaConfigValidator();
        var runtime = new KafkaConsumerRuntimeConfig(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" },
            new ExtendedConsumerSettings
            {
                StartMode = StartMode.RelativeTime,
                StartRelative = TimeSpan.Zero
            },
            new[] { "topic-1" },
            new KafkaSecuritySettings());

        var result = validator.ValidateConsumer(runtime);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.Contains("StartRelative", StringComparison.OrdinalIgnoreCase));
    }
}
