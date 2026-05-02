using System;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Restart;
using KoreForge.Kafka.Producer.Runtime;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Restart;

public sealed class DefaultProducerRestartPolicyTests
{
    private static ProducerFailureContext CreateContext(int failureCount)
        => new("topic", 0, failureCount, new InvalidOperationException(), DateTimeOffset.UtcNow);

    [Fact]
    public void Evaluate_TripsCircuitBreaker_WhenAttemptsExceeded()
    {
        var settings = new ProducerRestartSettings { MaxAttemptsInWindow = 2 };
        var policy = new DefaultProducerRestartPolicy(settings);

        var decision = policy.Evaluate(CreateContext(3));

        decision.IsCircuitBreakerTripped.Should().BeTrue();
        decision.ShouldRestart.Should().BeFalse();
    }

    [Fact]
    public void Evaluate_AppliesExponentialBackoff()
    {
        var settings = new ProducerRestartSettings
        {
            BackoffInitial = TimeSpan.FromMilliseconds(100),
            BackoffMultiplier = 2,
            BackoffMax = TimeSpan.FromSeconds(1),
            MaxAttemptsInWindow = 10
        };
        var policy = new DefaultProducerRestartPolicy(settings);

        var first = policy.Evaluate(CreateContext(1));
        var second = policy.Evaluate(CreateContext(2));
        var capped = policy.Evaluate(new ProducerFailureContext("topic", 0, 5, new Exception(), DateTimeOffset.UtcNow));

        first.DelayBeforeRestart.Should().Be(TimeSpan.FromMilliseconds(100));
        second.DelayBeforeRestart.Should().Be(TimeSpan.FromMilliseconds(200));
        capped.DelayBeforeRestart.Should().Be(settings.BackoffMax);
    }
}
