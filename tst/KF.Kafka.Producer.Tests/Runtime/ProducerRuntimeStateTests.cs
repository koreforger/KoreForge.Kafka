using System;
using FluentAssertions;
using KF.Kafka.Producer.Runtime;
using Xunit;

namespace KF.Kafka.Producer.Tests.Runtime;

public sealed class ProducerRuntimeStateTests
{
    [Fact]
    public void Constructor_CreatesRecords()
    {
        var state = new ProducerRuntimeState(2);

        state.WorkerCount.Should().Be(2);
        state[0].Should().NotBeNull();
        state[1].Should().NotBeNull();
        state.Invoking(s => s[-1]).Should().Throw<ArgumentOutOfRangeException>();
        state.Invoking(s => s[2]).Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Snapshot_ReturnsRecordSnapshots()
    {
        var state = new ProducerRuntimeState(1);
        state[0].RecordBatch(1, TimeSpan.FromMilliseconds(1), DateTime.Now);

        var snapshots = state.Snapshot();

        snapshots.Should().HaveCount(1);
        snapshots[0].TotalProducedCount.Should().Be(1);
    }

    [Fact]
    public void Constructor_ValidatesCount()
    {
        Action action = () => new ProducerRuntimeState(0);
        action.Should().Throw<ArgumentOutOfRangeException>();
    }
}
