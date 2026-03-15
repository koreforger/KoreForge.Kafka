using System;

namespace KF.Kafka.Producer.Restart;

public readonly record struct ProducerFailureContext(
    string Topic,
    int WorkerId,
    int FailureCount,
    Exception Exception,
    DateTimeOffset FailedAtUtc);

public readonly record struct ProducerRestartDecision(
    bool ShouldRestart,
    TimeSpan DelayBeforeRestart,
    bool IsCircuitBreakerTripped)
{
    public static ProducerRestartDecision Restart(TimeSpan delay) => new(true, delay, false);
    public static ProducerRestartDecision TripCircuitBreaker() => new(false, TimeSpan.Zero, true);
    public static ProducerRestartDecision GiveUp() => new(false, TimeSpan.Zero, false);
}

public interface IProducerRestartPolicy
{
    ProducerRestartDecision Evaluate(ProducerFailureContext context);
}
