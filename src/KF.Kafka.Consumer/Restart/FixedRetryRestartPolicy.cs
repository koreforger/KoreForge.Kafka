using System;

namespace KF.Kafka.Consumer.Restart;

/// <summary>
/// Simple restart policy that allows a bounded number of retries with a fixed backoff.
/// </summary>
public sealed class FixedRetryRestartPolicy : IRestartPolicy
{
    private readonly int _maxRetries;
    private readonly TimeSpan _backoff;

    public FixedRetryRestartPolicy(int maxRetries = 3, TimeSpan? backoff = null)
    {
        if (maxRetries < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRetries));
        }

        _maxRetries = maxRetries;
        _backoff = backoff ?? TimeSpan.FromSeconds(5);
    }

    public RestartDecision Evaluate(RestartContext context)
    {
        if (context.ConsecutiveFailures <= _maxRetries)
        {
            return RestartDecision.Restart(_backoff);
        }

        return RestartDecision.GiveUp;
    }
}
