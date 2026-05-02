using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Time;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Batch;

public sealed class KafkaRecordBatchTests
{
    [Fact]
    public void CreatedAt_exposes_timestamp()
    {
        var virtualClock = new VirtualSystemClock(DateTimeOffset.Parse("2025-01-01T09:30:00+02:00"));
        var batch = new KafkaRecordBatch(Array.Empty<ConsumeResult<byte[], byte[]>>(), virtualClock.Now);

        batch.CreatedAt.Should().Be(virtualClock.Now);
    }

    [Fact]
    public void CreatedAt_UtcDateTime_returns_utc()
    {
        var local = new DateTimeOffset(2025, 2, 2, 8, 0, 0, TimeSpan.FromHours(2));
        var batch = new KafkaRecordBatch(Array.Empty<ConsumeResult<byte[], byte[]>>(), local);

        batch.CreatedAt.UtcDateTime.Should().Be(local.UtcDateTime);
    }
}
