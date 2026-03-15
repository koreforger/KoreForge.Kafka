using FluentAssertions;
using KF.Kafka.Producer.Runtime;
using KF.Kafka.Producer.Workers;
using Xunit;

namespace KF.Kafka.Producer.Tests.Workers;

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
