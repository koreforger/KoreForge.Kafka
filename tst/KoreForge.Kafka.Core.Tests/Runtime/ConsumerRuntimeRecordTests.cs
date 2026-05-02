using FluentAssertions;
using KoreForge.Kafka.Core.Runtime;
using Xunit;

namespace KoreForge.Kafka.Core.Tests.Runtime;

public sealed class ConsumerRuntimeRecordTests
{
    [Fact]
    public void Snapshot_returns_last_recorded_values()
    {
        var record = new ConsumerRuntimeRecord();
        var now = new DateTime(2024, 01, 01, 12, 30, 00, DateTimeKind.Local);

        record.RecordBatch(10, TimeSpan.FromMilliseconds(25), now);
        record.RecordBatch(15, TimeSpan.FromMilliseconds(30), now.AddMinutes(1));

        var snapshot = record.Snapshot;

        snapshot.TotalConsumedCount.Should().Be(25);
        snapshot.LastBatchSize.Should().Be(15);
        snapshot.LastBatchDuration.Should().Be(TimeSpan.FromMilliseconds(30));
        snapshot.LastConsumedAtLocal.Should().Be(now.AddMinutes(1));
    }
}
