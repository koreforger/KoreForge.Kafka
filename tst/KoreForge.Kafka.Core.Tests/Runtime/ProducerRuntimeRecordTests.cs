using FluentAssertions;
using KoreForge.Kafka.Core.Runtime;
using Xunit;

namespace KoreForge.Kafka.Core.Tests.Runtime;

public sealed class ProducerRuntimeRecordTests
{
    [Fact]
    public void Snapshot_reflects_last_batch()
    {
        var record = new ProducerRuntimeRecord();
        var now = new DateTime(2024, 01, 01, 12, 30, 00, DateTimeKind.Local);

        record.RecordBatch(5, TimeSpan.FromMilliseconds(10), now);
        record.RecordBatch(7, TimeSpan.FromMilliseconds(12), now.AddSeconds(5));

        var snapshot = record.Snapshot;
        snapshot.TotalProducedCount.Should().Be(12);
        snapshot.LastBatchSize.Should().Be(7);
        snapshot.LastBatchDuration.Should().Be(TimeSpan.FromMilliseconds(12));
        snapshot.LastProducedAtLocal.Should().Be(now.AddSeconds(5));
    }
}
