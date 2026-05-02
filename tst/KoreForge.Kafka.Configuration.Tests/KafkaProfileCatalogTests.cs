using System;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Exceptions;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Profiles;
using KoreForge.Kafka.Configuration.Tests.Support;
using Xunit;

namespace KoreForge.Kafka.Configuration.Tests;

public sealed class KafkaProfileCatalogTests
{
    [Fact]
    public void GetProfileNames_returns_all_registered_names()
    {
        var options = CreateOptions();
        var monitor = new TestOptionsMonitor<KafkaConfigurationRootOptions>(options);
        var catalog = new KafkaProfileCatalog(monitor);

        catalog.GetProfileNames().Should().BeEquivalentTo("orders", "payments");
    }

    [Fact]
    public void GetProfile_returns_profile_when_present()
    {
        var options = CreateOptions();
        var monitor = new TestOptionsMonitor<KafkaConfigurationRootOptions>(options);
        var catalog = new KafkaProfileCatalog(monitor);

        var profile = catalog.GetProfile("orders");

        profile.Should().NotBeNull();
        profile.Cluster.Should().Be("cluster-a");
        profile.Type.Should().Be(KafkaClientType.Consumer);
    }

    [Fact]
    public void GetProfile_throws_when_unknown()
    {
        var options = CreateOptions();
        var monitor = new TestOptionsMonitor<KafkaConfigurationRootOptions>(options);
        var catalog = new KafkaProfileCatalog(monitor);

        Action act = () => catalog.GetProfile("missing");
        act.Should().Throw<KafkaProfileNotFoundException>().Which.ProfileName.Should().Be("missing");
    }

    private static KafkaConfigurationRootOptions CreateOptions()
    {
        var options = new KafkaConfigurationRootOptions();
        options.Profiles["orders"] = new KafkaProfileSettings
        {
            Cluster = "cluster-a",
            Type = KafkaClientType.Consumer,
            Topics = { "orders" }
        };

        options.Profiles["payments"] = new KafkaProfileSettings
        {
            Cluster = "cluster-b",
            Type = KafkaClientType.Producer,
            Topics = { "payments" }
        };

        return options;
    }
}
