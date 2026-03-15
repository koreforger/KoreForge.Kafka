using System;
using FluentAssertions;
using KF.Kafka.Consumer.Pipelines;
using KF.Kafka.Consumer.Tests.Support;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Pipelines;

public sealed class NoOpKafkaPipelineIntegrationMetricsTests
{
    [Fact]
    public void TrackBatch_ReturnsSingletonScope()
    {
        var metrics = NoOpKafkaPipelineIntegrationMetrics.Instance;
        var batch = KafkaRecordBatchFactory.CreateBatch(1, 2, 3);

        var scope = metrics.TrackBatch("test-pipeline", batch);

        scope.Should().BeSameAs(metrics);
        Action markFailure = () => scope.MarkFailed(new InvalidOperationException("boom"));
        markFailure.Should().NotThrow();
        Action dispose = scope.Dispose;
        dispose.Should().NotThrow();
    }
}
