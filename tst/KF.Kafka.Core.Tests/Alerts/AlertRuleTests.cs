using FluentAssertions;
using KF.Kafka.Core.Alerts;
using KF.Time;
using Xunit;

namespace KF.Kafka.Core.Tests.Alerts;

public sealed class AlertRuleTests
{
    [Fact]
    public void Fires_on_first_true_condition()
    {
        var fired = 0;
        var clock = new TestClock();
        var rule = new AlertRule<int>(x => x > 10, _ => fired++, clock: clock);

        rule.Evaluate(5);
        fired.Should().Be(0);

        rule.Evaluate(42);
        fired.Should().Be(1);
    }

    [Fact]
    public void Respects_transition_only_option()
    {
        var fired = 0;
        var clock = new TestClock();
        var rule = new AlertRule<int>(
            x => x > 0,
            _ => fired++,
            new AlertOptions { FireOnTransitionOnly = true },
            clock);

        rule.Evaluate(1);
        rule.Evaluate(2);

        fired.Should().Be(1, "second evaluation is still true but should not fire");

        rule.Evaluate(0);
        rule.Evaluate(2);
        fired.Should().Be(2, "transition from false to true should fire again");
    }

    [Fact]
    public void Enforces_min_interval_between_firings()
    {
        var fired = 0;
        var clock = new TestClock();
        var rule = new AlertRule<int>(
            _ => true,
            _ => fired++,
            new AlertOptions { FireOnTransitionOnly = false, MinIntervalBetweenFirings = TimeSpan.FromSeconds(10) },
            clock);

        rule.Evaluate(0);
        fired.Should().Be(1);

        rule.Evaluate(0);
        fired.Should().Be(1, "interval has not elapsed");

        clock.Advance(TimeSpan.FromSeconds(11));
        rule.Evaluate(0);
        fired.Should().Be(2);
    }

    private sealed class TestClock : ISystemClock
    {
        public DateTimeOffset UtcNow { get; private set; } = DateTimeOffset.Parse("2024-01-01T00:00:00Z");

        public DateTimeOffset Now => UtcNow.ToLocalTime();

        public long TimestampTicks => UtcNow.Ticks;

        public void Advance(TimeSpan delta) => UtcNow += delta;
    }
}
