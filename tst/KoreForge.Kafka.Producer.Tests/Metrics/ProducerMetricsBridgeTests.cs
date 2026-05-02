using System;
using System.Linq;
using System.Reflection;
using FluentAssertions;
using KoreForge.Kafka.Producer.Metrics;
using KoreForge.Metrics;
using NSubstitute;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Metrics;

public sealed class ProducerMetricsBridgeTests
{
    [Fact]
    public void TrackEnqueue_EmitsTags()
    {
        var (monitor, getTags, getOperationName) = CreateMonitorStub();
        var bridge = new ProducerMetricsBridge(monitor);

        using var scope = bridge.TrackEnqueue("topic-a", buffered: 3, capacity: 9);

        getOperationName().Should().Be("kafka.producer.enqueue");
        var tags = getTags();
        tags.Should().NotBeNull();
        tags!["topic"].Should().Be("topic-a");
        tags["buffered"].Should().Be("3");
        tags["capacity"].Should().Be("9");
        scope.MarkFailed();
    }

    [Fact]
    public void Trackers_ReturnNoop_WhenMonitorMissing()
    {
        var bridge = new ProducerMetricsBridge(null);

        var enqueue = bridge.TrackEnqueue("topic", 1, 1);
        var send = bridge.TrackSend("topic", 2);
        var persist = bridge.TrackBacklogPersist("topic", 3);
        var restore = bridge.TrackBacklogRestore("topic", 4);
        var alert = bridge.TrackAlert("test");
        var restart = bridge.TrackRestartDecision("topic", "restart");

        enqueue.Should().BeEquivalentTo(ProducerMetricScope.Noop);
        send.Should().BeEquivalentTo(ProducerMetricScope.Noop);
        persist.Should().BeEquivalentTo(ProducerMetricScope.Noop);
        restore.Should().BeEquivalentTo(ProducerMetricScope.Noop);
        alert.Should().BeEquivalentTo(ProducerMetricScope.Noop);
        restart.Should().BeEquivalentTo(ProducerMetricScope.Noop);
    }

    [Fact]
    public void TrackAlert_EmitsAlertName()
    {
        var (monitor, getTags, getOperationName) = CreateMonitorStub();
        var bridge = new ProducerMetricsBridge(monitor);

        using var _ = bridge.TrackAlert("buffer-high");

        getOperationName().Should().Be("kafka.producer.alert");
        var tags = getTags();
        tags.Should().NotBeNull();
        tags!["alert"].Should().Be("buffer-high");
    }

    [Fact]
    public void TrackRestartDecision_EmitsDecision()
    {
        var (monitor, getTags, getOperationName) = CreateMonitorStub();
        var bridge = new ProducerMetricsBridge(monitor);

        using var _ = bridge.TrackRestartDecision("topic-a", "restart");

        getOperationName().Should().Be("kafka.producer.restart");
        var tags = getTags();
        tags.Should().NotBeNull();
        tags!["topic"].Should().Be("topic-a");
        tags["decision"].Should().Be("restart");
    }

    private static (IOperationMonitor Monitor, Func<OperationTags?> GetTags, Func<string> GetOperationName) CreateMonitorStub()
    {
        var monitor = Substitute.For<IOperationMonitor>();
        OperationTags? capturedTags = null;
        var capturedName = string.Empty;
        monitor.Begin(Arg.Any<string>(), Arg.Any<OperationTags>())
               .Returns(ci =>
               {
                   capturedName = ci.ArgAt<string>(0);
                   capturedTags = ci.ArgAt<OperationTags>(1);
                   return (OperationScope)typeof(OperationScope)
                       .GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
                                       binder: null,
                                       types: Type.EmptyTypes,
                                       modifiers: null)!
                       .Invoke(null);
             });

         return (monitor, () => capturedTags, () => capturedName);
    }
}
