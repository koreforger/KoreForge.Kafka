using System;
using System.Collections.Generic;
using KF.Metrics;

namespace KF.Kafka.AdminClient.Instrumentation;

/// <summary>
/// Bridges <see cref="IKafkaAdminMetrics"/> to <see cref="IOperationMonitor"/> so admin operations automatically emit KF.Metrics events.
/// </summary>
public sealed class KafkaAdminOperationMonitorMetrics : IKafkaAdminMetrics
{
    private const string OperationPrefix = "kafka.admin.";
    private const string ErrorOperationName = "kafka.admin.errors";
    private const string CacheHitOperationName = "kafka.admin.cache.hit";
    private const string CacheMissOperationName = "kafka.admin.cache.miss";

    private readonly IOperationMonitor _monitor;

    public KafkaAdminOperationMonitorMetrics(IOperationMonitor monitor)
    {
        _monitor = monitor ?? throw new ArgumentNullException(nameof(monitor));
    }

    public IKafkaAdminCallScope TrackCall(string operation, string? topic, string? groupId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        var scope = _monitor.Begin(string.Concat(OperationPrefix, operation), BuildOperationTags(topic, groupId));
        return new OperationScopeAdapter(this, scope, operation, topic, groupId);
    }

    public void RecordCacheHit(string cacheName) => EmitCacheMetric(CacheHitOperationName, cacheName);

    public void RecordCacheMiss(string cacheName) => EmitCacheMetric(CacheMissOperationName, cacheName);

    private void EmitCacheMetric(string operationName, string cacheName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);
        using var scope = _monitor.Begin(operationName, BuildCacheTags(cacheName));
    }

    private void RecordErrorEvent(string operation, string? topic, string? groupId, Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var tags = BuildErrorTags(operation, topic, groupId, exception);
        using var scope = _monitor.Begin(ErrorOperationName, tags);
    }

    private static OperationTags? BuildOperationTags(string? topic, string? groupId)
    {
        Dictionary<string, string>? tags = null;
        if (!string.IsNullOrWhiteSpace(topic))
        {
            tags ??= new Dictionary<string, string>(StringComparer.Ordinal);
            tags["topic"] = topic!;
        }

        if (!string.IsNullOrWhiteSpace(groupId))
        {
            tags ??= new Dictionary<string, string>(StringComparer.Ordinal);
            tags["group"] = groupId!;
        }

        return tags is null ? null : new OperationTags(tags);
    }

    private static OperationTags BuildCacheTags(string cacheName)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["cache"] = cacheName
        };

        return new OperationTags(tags);
    }

    private static OperationTags BuildErrorTags(string operation, string? topic, string? groupId, Exception exception)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["operation"] = operation,
            ["exception"] = exception.GetType().Name
        };

        if (!string.IsNullOrWhiteSpace(topic))
        {
            tags["topic"] = topic!;
        }

        if (!string.IsNullOrWhiteSpace(groupId))
        {
            tags["group"] = groupId!;
        }

        return new OperationTags(tags);
    }

    private sealed class OperationScopeAdapter : IKafkaAdminCallScope
    {
        private readonly KafkaAdminOperationMonitorMetrics _owner;
        private readonly OperationScope _scope;
        private readonly string _operation;
        private readonly string? _topic;
        private readonly string? _groupId;
        private bool _errorRecorded;

        public OperationScopeAdapter(
            KafkaAdminOperationMonitorMetrics owner,
            OperationScope scope,
            string operation,
            string? topic,
            string? groupId)
        {
            _owner = owner;
            _scope = scope;
            _operation = operation;
            _topic = topic;
            _groupId = groupId;
        }

        public void MarkError(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            if (_errorRecorded)
            {
                return;
            }

            _scope.MarkFailed();
            _owner.RecordErrorEvent(_operation, _topic, _groupId, exception);
            _errorRecorded = true;
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }
}
