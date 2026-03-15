using System;
using FluentAssertions;
using KF.Kafka.Producer.Runtime;
using Xunit;

namespace KF.Kafka.Producer.Tests.Runtime;

public sealed class ProducerRuntimeRecordTests
{
    [Fact]
    public void RecordBatch_UpdatesSnapshot()
    {
        var record = new ProducerRuntimeRecord();
        var producedAt = DateTime.Now;

        record.RecordBatch(batchSize: 3, TimeSpan.FromMilliseconds(25), producedAt);
        var snapshot = record.Snapshot;

        snapshot.TotalProducedCount.Should().Be(3);
        snapshot.LastBatchSize.Should().Be(3);
        snapshot.LastBatchDuration.Should().Be(TimeSpan.FromMilliseconds(25));
        snapshot.LastProducedAtLocal.Should().BeCloseTo(producedAt, TimeSpan.FromMilliseconds(5));
        snapshot.State.Should().Be(ProducerWorkerState.Running);
    }

    [Fact]
    public void RecordFailureAndStopped_SetState()
    {
        var record = new ProducerRuntimeRecord();

        record.RecordFailure();
        record.Snapshot.State.Should().Be(ProducerWorkerState.Failed);

        record.RecordStopped();
        record.Snapshot.State.Should().Be(ProducerWorkerState.Stopped);
    }

    [Fact]
    public void RecordBatch_ValidatesInput()
    {
        var record = new ProducerRuntimeRecord();

        Action negativeBatch = () => record.RecordBatch(-1, TimeSpan.Zero, DateTime.Now);
        Action negativeDuration = () => record.RecordBatch(0, TimeSpan.FromMilliseconds(-1), DateTime.Now);

        negativeBatch.Should().Throw<ArgumentOutOfRangeException>();
        negativeDuration.Should().Throw<ArgumentOutOfRangeException>();
    }
}
