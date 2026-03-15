namespace KF.Kafka.Core.Alerts;

/// <summary>
/// Controls how often alert actions are allowed to fire.
/// </summary>
public sealed record AlertOptions
{
    /// <summary>
    /// Optional friendly name used only for logging/debugging.
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    /// When true the alert action only fires on false -&gt; true transitions of the condition.
    /// Defaults to true to prevent noisy alerts.
    /// </summary>
    public bool FireOnTransitionOnly { get; init; } = true;

    /// <summary>
    /// Minimum duration that has to elapse between consecutive firings while the condition stays true.
    /// </summary>
    public TimeSpan? MinIntervalBetweenFirings { get; init; }
}
