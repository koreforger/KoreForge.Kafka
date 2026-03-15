using System;
using FluentAssertions;
using KF.Kafka.Consumer.Batch;
using KF.Kafka.Consumer.Pipelines;
using KF.Kafka.Consumer.Tests.Support;
using KF.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Pipelines;

public sealed class KafkaPipelineOperationMonitorMetricsTests
{
    [Fact]
    public void TrackBatch_records_pipeline_steps_and_failures()
    {
        using var provider = CreateProvider();
        var monitor = provider.GetRequiredService<IOperationMonitor>();
        var engine = provider.GetRequiredService<MonitoringEngine>();
        var metrics = new KafkaPipelineOperationMonitorMetrics(monitor);

        using (var batchScope = metrics.TrackBatch("orders", 3, isSequential: true, maxDegreeOfParallelism: 1))
        {
            using (var stepScope = batchScope.TrackStep("transform", 3, isBatchAware: true))
            {
                stepScope.RecordOutcome(2);
                stepScope.MarkFailed(new InvalidOperationException("step"));
            }

            batchScope.MarkFailed(new InvalidOperationException("batch"));
        }

        var batchSnapshot = engine.TryGetMetrics("kafka.pipeline.batch")!.CaptureSnapshot();
        batchSnapshot.TotalCount.Should().Be(1);
        batchSnapshot.TotalFailures.Should().Be(1);

        var stepSnapshot = engine.TryGetMetrics("kafka.pipeline.step")!.CaptureSnapshot();
        stepSnapshot.TotalCount.Should().Be(1);
        stepSnapshot.TotalFailures.Should().Be(1);

        var abortSnapshot = engine.TryGetMetrics("kafka.pipeline.step.abort")!.CaptureSnapshot();
        abortSnapshot.TotalCount.Should().Be(1);
    }

    [Fact]
    public void TrackIntegrationBatch_records_success_and_failure()
    {
        using var provider = CreateProvider();
        var monitor = provider.GetRequiredService<IOperationMonitor>();
        var engine = provider.GetRequiredService<MonitoringEngine>();
        var metrics = new KafkaPipelineOperationMonitorMetrics(monitor);
        var batch = KafkaRecordBatchFactory.CreateBatch(1, 2, 3);

        using (metrics.TrackBatch("orders", batch))
        {
            // success path
        }

        using (var scope = metrics.TrackBatch("orders", batch))
        {
            scope.MarkFailed(new InvalidOperationException("integration"));
        }

        var snapshot = engine.TryGetMetrics("kafka.consumer.pipeline.batch")!.CaptureSnapshot();
        snapshot.TotalCount.Should().Be(2);
        snapshot.TotalFailures.Should().Be(1);
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
