using System;
using FluentAssertions;
using KF.Kafka.AdminClient.Instrumentation;
using KF.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace KF.Kafka.AdminClient.Tests;

public sealed class KafkaAdminOperationMonitorMetricsTests
{
    [Fact]
    public void TrackCall_records_success_and_failure()
    {
        using var provider = CreateProvider();
        var monitor = provider.GetRequiredService<IOperationMonitor>();
        var engine = provider.GetRequiredService<MonitoringEngine>();
        var metrics = new KafkaAdminOperationMonitorMetrics(monitor);

        using (metrics.TrackCall("metadata", "orders", "orders-group"))
        {
            // success path
        }

        using (var scope = metrics.TrackCall("metadata", "orders", "orders-group"))
        {
            scope.MarkError(new InvalidOperationException("boom"));
        }

        var operationMetrics = engine.TryGetMetrics("kafka.admin.metadata");
        operationMetrics.Should().NotBeNull();
        var snapshot = operationMetrics!.CaptureSnapshot();
        snapshot.TotalCount.Should().Be(2);
        snapshot.TotalFailures.Should().Be(1);

        var errorMetrics = engine.TryGetMetrics("kafka.admin.errors");
        errorMetrics.Should().NotBeNull();
        errorMetrics!.CaptureSnapshot().TotalCount.Should().Be(1);
    }

    [Fact]
    public void Cache_metrics_record_hits_and_misses()
    {
        using var provider = CreateProvider();
        var monitor = provider.GetRequiredService<IOperationMonitor>();
        var engine = provider.GetRequiredService<MonitoringEngine>();
        var metrics = new KafkaAdminOperationMonitorMetrics(monitor);

        metrics.RecordCacheHit("metadata");
        metrics.RecordCacheMiss("lag");

        var hitSnapshot = engine.TryGetMetrics("kafka.admin.cache.hit")!.CaptureSnapshot();
        hitSnapshot.TotalCount.Should().Be(1);

        var missSnapshot = engine.TryGetMetrics("kafka.admin.cache.miss")!.CaptureSnapshot();
        missSnapshot.TotalCount.Should().Be(1);
    }

    [Fact]
    public void MarkError_is_idempotent_per_scope()
    {
        using var provider = CreateProvider();
        var monitor = provider.GetRequiredService<IOperationMonitor>();
        var engine = provider.GetRequiredService<MonitoringEngine>();
        var metrics = new KafkaAdminOperationMonitorMetrics(monitor);

        using (var scope = metrics.TrackCall("metadata", null, null))
        {
            scope.MarkError(new InvalidOperationException("boom"));
            scope.MarkError(new InvalidOperationException("ignored"));
        }

        var operationSnapshot = engine.TryGetMetrics("kafka.admin.metadata")!.CaptureSnapshot();
        operationSnapshot.TotalCount.Should().Be(1);
        operationSnapshot.TotalFailures.Should().Be(1);

        var errorSnapshot = engine.TryGetMetrics("kafka.admin.errors")!.CaptureSnapshot();
        errorSnapshot.TotalCount.Should().Be(1);
    }

    private static ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();
        services.AddKoreForgeMetrics(options =>
        {
            options.EventDispatchMode = EventDispatchMode.Inline;
            options.SamplingRate = 1;
        });

        return services.BuildServiceProvider();
    }
}
