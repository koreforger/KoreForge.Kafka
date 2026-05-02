using System;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Backpressure;
using KoreForge.Kafka.Producer.Tests.Support;
using Microsoft.Extensions.Logging;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Backpressure;

public sealed class DefaultProducerBackpressurePolicyTests
{
    private static ProducerBackpressureContext CreateContext(ProducerBackpressureMode mode)
        => new("topic-a", 42, 100, DateTimeOffset.UtcNow, mode);

    [Theory]
    [InlineData(ProducerBackpressureMode.Block, true, false, false)]
    [InlineData(ProducerBackpressureMode.Drop, false, true, false)]
    [InlineData(ProducerBackpressureMode.Fail, false, false, true)]
    public void Evaluate_ReturnsExpectedDecision(ProducerBackpressureMode mode, bool shouldBlock, bool shouldDrop, bool shouldFail)
    {
        var loggerFactory = new RecordingLoggerFactory();
        var policy = new DefaultProducerBackpressurePolicy(loggerFactory.CreateLogger("backpressure"));

        var decision = policy.Evaluate(CreateContext(mode));

        decision.Should().BeEquivalentTo(new ProducerBackpressureDecision(shouldBlock, shouldDrop, shouldFail));
    }

    [Fact]
    public void LogDecision_LogsWarning_ForDrop()
    {
        var factory = new RecordingLoggerFactory();
        var policy = new DefaultProducerBackpressurePolicy(factory.CreateLogger("backpressure"));
        var context = CreateContext(ProducerBackpressureMode.Drop);

        policy.LogDecision(context, ProducerBackpressureDecision.Drop());

        factory.Entries.Should().Contain(entry =>
            entry.Level == LogLevel.Warning &&
            entry.Message.Contains("backpressure", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void LogDecision_LogsError_ForFail()
    {
        var factory = new RecordingLoggerFactory();
        var policy = new DefaultProducerBackpressurePolicy(factory.CreateLogger("backpressure"));
        var context = CreateContext(ProducerBackpressureMode.Fail);

        policy.LogDecision(context, ProducerBackpressureDecision.Fail());

        factory.Entries.Should().Contain(entry => entry.Level == LogLevel.Error);
    }

    [Fact]
    public void LogDecision_NoOp_ForBlock()
    {
        var factory = new RecordingLoggerFactory();
        var policy = new DefaultProducerBackpressurePolicy(factory.CreateLogger("backpressure"));
        var context = CreateContext(ProducerBackpressureMode.Block);

        policy.LogDecision(context, ProducerBackpressureDecision.Block());

        factory.Entries.Should().BeEmpty();
    }
}
