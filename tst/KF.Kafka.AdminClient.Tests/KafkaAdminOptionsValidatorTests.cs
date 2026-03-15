using FluentAssertions;
using KF.Kafka.AdminClient.Internal;
using KF.Kafka.Configuration.Admin;
using Microsoft.Extensions.Options;
using Xunit;

namespace KF.Kafka.AdminClient.Tests;

public sealed class KafkaAdminOptionsValidatorTests
{
    [Fact]
    public void Valid_options_pass_validation()
    {
        var validator = new KafkaAdminOptionsValidator();
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "localhost:9092",
            RequestTimeoutMs = 1_000,
            MaxDegreeOfParallelism = 4,
            Retry = new KafkaAdminRetryOptions { MaxRetries = 1, InitialBackoffMs = 10, MaxBackoffMs = 100 }
        };

        var result = validator.Validate(string.Empty, options);
        result.Succeeded.Should().BeTrue(result.FailureMessage);
    }

    [Fact]
    public void Missing_bootstrap_servers_fail_validation()
    {
        var validator = new KafkaAdminOptionsValidator();
        var options = new KafkaAdminOptions
        {
            BootstrapServers = string.Empty,
            RequestTimeoutMs = 1
        };

        var result = validator.Validate(string.Empty, options);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("BootstrapServers");
    }

    [Fact]
    public void Invalid_parallelism_fails_validation()
    {
        var validator = new KafkaAdminOptionsValidator();
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "localhost:9092",
            RequestTimeoutMs = 1_000,
            MaxDegreeOfParallelism = 0
        };

        var result = validator.Validate(string.Empty, options);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("MaxDegreeOfParallelism");
    }

    [Fact]
    public void Invalid_retry_window_fails_validation()
    {
        var validator = new KafkaAdminOptionsValidator();
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "localhost:9092",
            RequestTimeoutMs = 1_000,
            Retry = new KafkaAdminRetryOptions { MaxRetries = 2, InitialBackoffMs = 200, MaxBackoffMs = 100 }
        };

        var result = validator.Validate(string.Empty, options);
        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("MaxBackoffMs");
    }
}
