using System;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Restart;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Restart;

public sealed class FixedRetryRestartPolicyTests
{
    [Fact]
    public void Constructor_Validates_MaxRetries()
    {
        Action act = () => new FixedRetryRestartPolicy(maxRetries: -1);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be("maxRetries");
    }

    [Fact]
    public void Evaluate_Allows_Retries_Within_Limit()
    {
        var policy = new FixedRetryRestartPolicy(maxRetries: 2, backoff: TimeSpan.FromSeconds(3));
        var context = new RestartContext(
            WorkerId: 1,
            ConsecutiveFailures: 2,
            Exception: new InvalidOperationException("boom"),
            FailedAtUtc: DateTimeOffset.UtcNow);

        var decision = policy.Evaluate(context);

        decision.Action.Should().Be(RestartAction.Restart);
        decision.Delay.Should().Be(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void Evaluate_GivesUp_When_FailureLimitExceeded()
    {
        var policy = new FixedRetryRestartPolicy(maxRetries: 1, backoff: TimeSpan.Zero);
        var context = new RestartContext(
            WorkerId: 2,
            ConsecutiveFailures: 3,
            Exception: new Exception("fatal"),
            FailedAtUtc: DateTimeOffset.UtcNow);

        var decision = policy.Evaluate(context);

        decision.Should().Be(RestartDecision.GiveUp);
    }
}
