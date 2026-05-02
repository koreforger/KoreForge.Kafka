using System;
using System.Collections.Generic;
using KoreForge.Kafka.Core.Alerts;
using KoreForge.Kafka.Producer.Metrics;
using KoreForge.Kafka.Producer.Runtime;
using KoreForge.Time;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Producer.Alerts;

public sealed class ProducerAlertRegistry : IProducerAlertRegistrar
{
    private readonly List<AlertRule<ProducerStatusSnapshot>> _alerts = new();
    private readonly ISystemClock _clock;
    private ILogger<ProducerAlertRegistry>? _logger;
    private ProducerMetricsBridge? _metrics;

    public ProducerAlertRegistry(ISystemClock? clock = null)
    {
        _clock = clock ?? SystemClock.Instance;
    }

    internal void AttachObservers(ILogger<ProducerAlertRegistry>? logger, ProducerMetricsBridge? metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    public IReadOnlyList<AlertRule<ProducerStatusSnapshot>> Alerts => _alerts;

    public IProducerAlertRegistrar RegisterAlert(
        Func<ProducerStatusSnapshot, bool> condition,
        Action<ProducerStatusSnapshot> action,
        AlertOptions? options = null)
    {
        var alertName = options?.Name ?? $"alert-{_alerts.Count + 1}";
        Action<ProducerStatusSnapshot> wrappedAction = snapshot =>
        {
            var scope = _metrics?.TrackAlert(alertName) ?? ProducerMetricScope.Noop;
            try
            {
                _logger?.LogWarning("Kafka producer alert {AlertName} triggered", alertName);
                action(snapshot);
            }
            catch
            {
                scope.MarkFailed();
                throw;
            }
            finally
            {
                scope.Dispose();
            }
        };

        var rule = new AlertRule<ProducerStatusSnapshot>(condition, wrappedAction, options, _clock);
        _alerts.Add(rule);
        return this;
    }

    public void Evaluate(ProducerStatusSnapshot snapshot)
    {
        foreach (var alert in _alerts)
        {
            alert.Evaluate(snapshot);
        }
    }
}
