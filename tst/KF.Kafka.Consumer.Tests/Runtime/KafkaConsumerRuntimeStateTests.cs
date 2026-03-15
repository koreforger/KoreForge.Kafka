using System;
using System.Linq;
using FluentAssertions;
using KF.Kafka.Consumer.Runtime;
using KF.Kafka.Core.Runtime;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Runtime;

public sealed class KafkaConsumerRuntimeStateTests
{
    [Fact]
    public void CreateSnapshot_ProducesRecordsForEachWorker()
    {
        var state = new KafkaConsumerRuntimeState(workerCount: 3);

        state[0].RecordBatch(5, TimeSpan.FromMilliseconds(10), DateTime.Now);
        state[1].RecordBatch(7, TimeSpan.FromMilliseconds(20), DateTime.Now);
        state[2].RecordBatch(0, TimeSpan.Zero, DateTime.Now);

        var snapshot = state.CreateSnapshot();

        snapshot.Workers.Should().HaveCount(3);
        snapshot.TotalConsumed.Should().Be(12);
        snapshot.Workers.Select(w => w.LastBatchSize).Should().Contain(new[] { 5, 7, 0 });
    }

    [Fact]
    public void Constructor_Throws_WhenWorkerCountLessThanOne()
    {
        Action act = () => new KafkaConsumerRuntimeState(workerCount: 0);

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be("workerCount");
    }

    [Fact]
    public void CreateSnapshot_ComputesAggregateMetrics()
    {
        var state = new KafkaConsumerRuntimeState(workerCount: 2);

        state[0].RecordBatch(3, TimeSpan.FromMilliseconds(5), DateTime.Now);
        state[0].UpdateState(ConsumerWorkerState.Paused);
        state[1].RecordBatch(1, TimeSpan.FromMilliseconds(1), DateTime.Now);
        state[1].UpdateState(ConsumerWorkerState.Failed);

        var snapshot = state.CreateSnapshot();

        snapshot.TotalConsumed.Should().Be(4);
        snapshot.PausedWorkerCount.Should().Be(1);
        snapshot.PercentagePausedConsumers.Should().Be(50);
        snapshot.AnyWorkerFailed.Should().BeTrue();
        snapshot.FailedWorkerCount.Should().Be(1);
    }
}
