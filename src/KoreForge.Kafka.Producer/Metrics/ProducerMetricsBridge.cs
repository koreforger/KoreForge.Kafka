using System;
using System.Collections.Generic;
using System.Globalization;
using KoreForge.Metrics;

namespace KoreForge.Kafka.Producer.Metrics;

internal sealed class ProducerMetricsBridge
{
    private readonly IOperationMonitor? _monitor;

    public ProducerMetricsBridge(IOperationMonitor? monitor)
    {
        _monitor = monitor;
    }

    public ProducerMetricScope TrackEnqueue(string topic, long buffered, int capacity)
    {
        if (_monitor is null)
        {
            return ProducerMetricScope.Noop;
        }

        var tags = new Dictionary<string, string>
        {
            ["topic"] = topic,
            ["buffered"] = buffered.ToString(CultureInfo.InvariantCulture),
            ["capacity"] = capacity.ToString(CultureInfo.InvariantCulture)
        };

        var scope = _monitor.Begin("kafka.producer.enqueue", new OperationTags(tags));
        return new ProducerMetricScope(scope);
    }

    public ProducerMetricScope TrackSend(string topic, int batchSize)
    {
        if (_monitor is null)
        {
            return ProducerMetricScope.Noop;
        }

        var tags = new Dictionary<string, string>
        {
            ["topic"] = topic,
            ["batch.size"] = batchSize.ToString(CultureInfo.InvariantCulture)
        };

        var scope = _monitor.Begin("kafka.producer.send", new OperationTags(tags));
        return new ProducerMetricScope(scope);
    }

    public ProducerMetricScope TrackBacklogPersist(string topic, int count)
    {
        if (_monitor is null)
        {
            return ProducerMetricScope.Noop;
        }

        var tags = new Dictionary<string, string>
        {
            ["topic"] = topic,
            ["count"] = count.ToString(CultureInfo.InvariantCulture)
        };

        var scope = _monitor.Begin("kafka.producer.backlog.persist", new OperationTags(tags));
        return new ProducerMetricScope(scope);
    }

    public ProducerMetricScope TrackBacklogRestore(string topic, int count)
    {
        if (_monitor is null)
        {
            return ProducerMetricScope.Noop;
        }

        var tags = new Dictionary<string, string>
        {
            ["topic"] = topic,
            ["count"] = count.ToString(CultureInfo.InvariantCulture)
        };

        var scope = _monitor.Begin("kafka.producer.backlog.restore", new OperationTags(tags));
        return new ProducerMetricScope(scope);
    }

    public ProducerMetricScope TrackAlert(string alertName)
    {
        if (_monitor is null)
        {
            return ProducerMetricScope.Noop;
        }

        var tags = new Dictionary<string, string>
        {
            ["alert"] = alertName
        };

        var scope = _monitor.Begin("kafka.producer.alert", new OperationTags(tags));
        return new ProducerMetricScope(scope);
    }

    public ProducerMetricScope TrackRestartDecision(string topic, string decision)
    {
        if (_monitor is null)
        {
            return ProducerMetricScope.Noop;
        }

        var tags = new Dictionary<string, string>
        {
            ["topic"] = topic,
            ["decision"] = decision
        };

        var scope = _monitor.Begin("kafka.producer.restart", new OperationTags(tags));
        return new ProducerMetricScope(scope);
    }
}

internal readonly struct ProducerMetricScope : IDisposable
{
    private readonly OperationScope? _scope;

    public static ProducerMetricScope Noop => new(null);

    public ProducerMetricScope(OperationScope? scope)
    {
        _scope = scope;
    }

    public void MarkFailed()
    {
        _scope?.MarkFailed();
    }

    public void Dispose()
    {
        _scope?.Dispose();
    }
}
