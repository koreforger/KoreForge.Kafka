using System;

namespace KF.Kafka.Consumer.Restart;

/// <summary>
/// Determines whether a worker should be restarted after a failure.
/// </summary>
public interface IRestartPolicy
{
    RestartDecision Evaluate(RestartContext context);
}

/// <summary>
/// Context supplied to a restart policy when a worker fails.
/// </summary>
/// <param name="WorkerId">1-based worker identifier.</param>
/// <param name="ConsecutiveFailures">Number of consecutive failures for this worker.</param>
/// <param name="Exception">The exception observed when the worker faulted.</param>
/// <param name="FailedAtUtc">Timestamp when the failure was observed.</param>
public readonly record struct RestartContext(
    int WorkerId,
    int ConsecutiveFailures,
    Exception Exception,
    DateTimeOffset FailedAtUtc);

/// <summary>
/// Decision returned by a restart policy.
/// </summary>
/// <param name="Action">Indicates how the supervisor should proceed.</param>
/// <param name="Delay">Optional delay before attempting a restart.</param>
public readonly record struct RestartDecision(RestartAction Action, TimeSpan Delay)
{
    public static RestartDecision None { get; } = new(RestartAction.None, TimeSpan.Zero);
    public static RestartDecision GiveUp { get; } = new(RestartAction.GiveUp, TimeSpan.Zero);
    public static RestartDecision Restart(TimeSpan delay) => new(RestartAction.Restart, delay);
}

public enum RestartAction
{
    None,
    Restart,
    GiveUp
}
