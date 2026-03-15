using System;
using KF.Kafka.Configuration.Producer;

namespace KF.Kafka.Producer.Restart;

public sealed class DefaultProducerRestartPolicy : IProducerRestartPolicy
{
    private readonly ProducerRestartSettings _settings;

    public DefaultProducerRestartPolicy(ProducerRestartSettings settings)
    {
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    }

    public ProducerRestartDecision Evaluate(ProducerFailureContext context)
    {
        if (context.FailureCount >= _settings.MaxAttemptsInWindow)
        {
            return ProducerRestartDecision.TripCircuitBreaker();
        }

        var multiplier = Math.Pow(_settings.BackoffMultiplier, Math.Max(0, context.FailureCount - 1));
        var delay = TimeSpan.FromMilliseconds(_settings.BackoffInitial.TotalMilliseconds * multiplier);
        if (delay > _settings.BackoffMax)
        {
            delay = _settings.BackoffMax;
        }

        return ProducerRestartDecision.Restart(delay);
    }
}
