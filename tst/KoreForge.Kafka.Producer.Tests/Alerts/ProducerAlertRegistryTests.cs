using System;
using System.Threading;
using FluentAssertions;
using KoreForge.Kafka.Core.Alerts;
using KoreForge.Kafka.Producer.Alerts;
using KoreForge.Kafka.Producer.Runtime;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Alerts;

public sealed class ProducerAlertRegistryTests
{
    [Fact]
    public void RegisterAlert_AddsRule()
    {
        var registry = new ProducerAlertRegistry();

        registry.RegisterAlert(_ => true, _ => { });

        registry.Alerts.Should().HaveCount(1);
    }

    [Fact]
    public void Evaluate_InvokesMatchingAlert()
    {
        var registry = new ProducerAlertRegistry();
        var invoked = 0;
        registry.RegisterAlert(snapshot => snapshot.TotalBufferedMessages > 0, _ => Interlocked.Increment(ref invoked));

        registry.Evaluate(new ProducerStatusSnapshot { TotalBufferedMessages = 10 });

        invoked.Should().Be(1);
    }
}
