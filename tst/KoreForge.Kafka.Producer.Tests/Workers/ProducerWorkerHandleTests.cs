using FluentAssertions;
using KoreForge.Kafka.Producer.Runtime;
using KoreForge.Kafka.Producer.Workers;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Workers;

public sealed class ProducerWorkerHandleTests
{
    [Fact]
    public void IncrementAndResetFailures_WorkAsExpected()
    {
        var handle = new ProducerWorkerHandle(1, "topic", new ProducerRuntimeRecord());

        handle.IncrementFailures().Should().Be(1);
        handle.IncrementFailures().Should().Be(2);

        handle.ResetFailures();
        handle.IncrementFailures().Should().Be(1);
    }
}
