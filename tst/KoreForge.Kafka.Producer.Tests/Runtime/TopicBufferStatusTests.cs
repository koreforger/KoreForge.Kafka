using FluentAssertions;
using KoreForge.Kafka.Producer.Runtime;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Runtime;

public sealed class TopicBufferStatusTests
{
    [Fact]
    public void BufferFillPercentage_ComputesRatio()
    {
        var status = new TopicBufferStatus
        {
            TopicName = "topic",
            BufferedMessages = 50,
            BufferCapacity = 100
        };

        status.BufferFillPercentage.Should().Be(50);
    }

    [Fact]
    public void BufferFillPercentage_ReturnsZero_WhenCapacityZero()
    {
        var status = new TopicBufferStatus
        {
            TopicName = "topic",
            BufferedMessages = 10,
            BufferCapacity = 0
        };

        status.BufferFillPercentage.Should().Be(0);
    }
}
