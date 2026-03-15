using System;
using FluentAssertions;
using KF.Kafka.Consumer.Backpressure;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Backpressure;

public sealed class ThresholdBackpressurePolicyTests
{
    [Theory]
    [InlineData(-0.1, 0.4)]
    [InlineData(1.1, 0.4)]
    public void Constructor_validates_pause_threshold(double pauseThreshold, double resumeThreshold)
    {
        Action act = () => new ThresholdBackpressurePolicy(pauseThreshold, resumeThreshold);
        act.Should().Throw<ArgumentOutOfRangeException>().And.ParamName.Should().Be("pauseThreshold");
    }

    [Theory]
    [InlineData(0.9, -0.1)]
    [InlineData(0.9, 1.1)]
    public void Constructor_validates_resume_threshold(double pauseThreshold, double resumeThreshold)
    {
        Action act = () => new ThresholdBackpressurePolicy(pauseThreshold, resumeThreshold);
        act.Should().Throw<ArgumentOutOfRangeException>().And.ParamName.Should().Be("resumeThreshold");
    }

    [Fact]
    public void Constructor_requires_resume_lower_than_pause()
    {
        Action act = () => new ThresholdBackpressurePolicy(0.5, 0.6);
        act.Should().Throw<ArgumentException>().And.ParamName.Should().Be("resumeThreshold");
    }

    [Fact]
    public void Evaluate_returns_pause_when_utilization_exceeds_threshold()
    {
        var policy = new ThresholdBackpressurePolicy(0.8, 0.4);
        var context = CreateContext(0.85);

        policy.Evaluate(context).Should().Be(BackpressureDecision.Pause);
    }

    [Fact]
    public void Evaluate_returns_resume_when_utilization_below_threshold()
    {
        var policy = new ThresholdBackpressurePolicy(0.8, 0.4);
        var context = CreateContext(0.2);

        policy.Evaluate(context).Should().Be(BackpressureDecision.Resume);
    }

    [Fact]
    public void Evaluate_returns_none_between_thresholds()
    {
        var policy = new ThresholdBackpressurePolicy(0.8, 0.4);
        var context = CreateContext(0.6);

        policy.Evaluate(context).Should().Be(BackpressureDecision.None);
    }

    [Fact]
    public void Evaluate_returns_none_when_capacity_not_positive()
    {
        var policy = new ThresholdBackpressurePolicy();
        var context = new BackpressureContext(WorkerId: 1, BacklogSize: 100, Capacity: 0, Utilization: 1);

        policy.Evaluate(context).Should().Be(BackpressureDecision.None);
    }

    [Fact]
    public void BackpressureContext_preserves_values()
    {
        var context = new BackpressureContext(WorkerId: 7, BacklogSize: 42, Capacity: 100, Utilization: 0.42);

        context.WorkerId.Should().Be(7);
        context.BacklogSize.Should().Be(42);
        context.Capacity.Should().Be(100);
        context.Utilization.Should().Be(0.42);
    }

    private static BackpressureContext CreateContext(double utilization, int capacity = 100)
    {
        var backlog = (int)Math.Round(utilization * capacity, MidpointRounding.AwayFromZero);
        return new BackpressureContext(WorkerId: 1, BacklogSize: backlog, Capacity: capacity, Utilization: utilization);
    }
}
