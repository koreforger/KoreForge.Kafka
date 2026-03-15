using FluentAssertions;
using KF.Kafka.Configuration.Admin;
using Xunit;

namespace KF.Kafka.Configuration.Tests;

public sealed class KafkaAdminOptionsTests
{
    [Fact]
    public void Defaults_are_initialized()
    {
        var options = new KafkaAdminOptions();

        options.BootstrapServers.Should().BeEmpty();
        options.ClientId.Should().BeNull();
        options.RequestTimeoutMs.Should().Be(10_000);
        options.MaxDegreeOfParallelism.Should().BeGreaterThan(0);
        options.InternalConsumerGroupPrefix.Should().Be("khaos-admin");

        options.Retry.Should().NotBeNull();
        options.Cache.Should().NotBeNull();
        options.LagHealth.Should().NotBeNull();
        options.Security.Should().NotBeNull();

        options.Retry.MaxRetries.Should().Be(3);
        options.Cache.LagCacheEnabled.Should().BeTrue();
        options.LagHealth.HealthyTotalLagThreshold.Should().Be(10_000);
        options.Security.SecurityProtocol.Should().BeNull();
    }

    [Fact]
    public void Options_are_mutable_for_custom_configuration()
    {
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "cluster:9092",
            ClientId = "custom-admin",
            RequestTimeoutMs = 5_000,
            MaxDegreeOfParallelism = 8,
            InternalConsumerGroupPrefix = "ops",
            Retry = new KafkaAdminRetryOptions { MaxRetries = 5, InitialBackoffMs = 500, MaxBackoffMs = 5_000 },
            Cache = new KafkaAdminCacheOptions { LagCacheEnabled = false, MetadataCacheTtlMs = 10_000 },
            LagHealth = new KafkaAdminLagHealthOptions { HealthyTotalLagThreshold = 500, WarningPartitionLagThreshold = 2_000 },
            Security = new KafkaAdminSecurityOptions { SecurityProtocol = "Ssl", SaslMechanism = "PLAIN", SaslUsername = "user" }
        };

        options.BootstrapServers.Should().Be("cluster:9092");
        options.ClientId.Should().Be("custom-admin");
        options.RequestTimeoutMs.Should().Be(5_000);
        options.MaxDegreeOfParallelism.Should().Be(8);
        options.InternalConsumerGroupPrefix.Should().Be("ops");

        options.Retry.MaxRetries.Should().Be(5);
        options.Retry.InitialBackoffMs.Should().Be(500);
        options.Retry.MaxBackoffMs.Should().Be(5_000);

        options.Cache.LagCacheEnabled.Should().BeFalse();
        options.Cache.MetadataCacheTtlMs.Should().Be(10_000);

        options.LagHealth.HealthyTotalLagThreshold.Should().Be(500);
        options.LagHealth.WarningPartitionLagThreshold.Should().Be(2_000);

        options.Security.SecurityProtocol.Should().Be("Ssl");
        options.Security.SaslMechanism.Should().Be("PLAIN");
        options.Security.SaslUsername.Should().Be("user");
    }
}
