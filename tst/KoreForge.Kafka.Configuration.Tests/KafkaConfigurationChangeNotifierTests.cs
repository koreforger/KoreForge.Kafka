using System;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Profiles;
using KoreForge.Kafka.Configuration.Tests.Support;
using Xunit;

namespace KoreForge.Kafka.Configuration.Tests;

public sealed class KafkaConfigurationChangeNotifierTests
{
    [Fact]
    public void Raises_event_when_options_change()
    {
        var monitor = new TestOptionsMonitor<KafkaConfigurationRootOptions>(new KafkaConfigurationRootOptions());
        var notifier = new KafkaConfigurationChangeNotifier(monitor);
        KafkaConfigurationRootOptions? observed = null;

        notifier.Changed += (_, args) => observed = args.NewOptions;

        var next = new KafkaConfigurationRootOptions();
        monitor.Update(next);

        observed.Should().BeSameAs(next);
    }

    [Fact]
    public void Dispose_unsubscribes_from_monitor()
    {
        var monitor = new TestOptionsMonitor<KafkaConfigurationRootOptions>(new KafkaConfigurationRootOptions());
        var notifier = new KafkaConfigurationChangeNotifier(monitor);
        var invocationCount = 0;
        notifier.Changed += (_, __) => invocationCount++;

        notifier.Dispose();
        monitor.Update(new KafkaConfigurationRootOptions());

        invocationCount.Should().Be(0);
    }
}
