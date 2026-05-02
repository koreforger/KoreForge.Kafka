using System;
using System.Linq;
using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Exceptions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Configuration.Generation;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Security;
using KoreForge.Kafka.Configuration.Testing;
using KoreForge.Kafka.Configuration.Validation;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace KoreForge.Kafka.Configuration.Tests;

public class KafkaClientConfigFactoryTests
{
    private const string Json = """
    {
      "Kafka": {
        "Defaults": {
          "GroupIdTemplate": "test-{ProfileName}-{Random4}"
        },
        "Clusters": {
          "Primary": {
            "BootstrapServers": "localhost:9092"
          }
        },
        "SecurityProfiles": {
          "None": {
            "Mode": "None"
          }
        },
        "Profiles": {
          "PaymentsConsumer": {
            "Type": "Consumer",
            "Cluster": "Primary",
            "SecurityProfile": "None",
            "Topics": ["payments"],
            "Confluent": {
              "enable.auto.commit": "false"
            },
            "ExtendedConsumer": {
              "ConsumerCount": 2,
              "StartMode": "Earliest"
            }
          }
        }
      }
    }
    """;

    [Fact]
    public void CreateConsumer_GeneratesGroupId_WhenMissing()
    {
        var factory = KafkaConfigurationTestHelper.CreateFactoryFromJson(Json);

        var runtime = factory.CreateConsumer("PaymentsConsumer");

        runtime.ConfluentConfig.GroupId.Should().NotBeNullOrWhiteSpace();
        runtime.Topics.Should().ContainSingle("payments");
    }

    [Fact]
    public void CreateConsumer_Throws_ForMissingProfile()
    {
        var factory = KafkaConfigurationTestHelper.CreateFactoryFromJson(Json);

        var act = () => factory.CreateConsumer("missing");

        act.Should().Throw<KafkaProfileNotFoundException>();
    }

    [Fact]
    public void CreateProducer_UsesClusterDefaultSecurityProfileWhenNotSpecified()
    {
      var options = new KafkaConfigurationRootOptions
      {
        Clusters =
        {
          ["primary"] = new KafkaClusterSettings
          {
            BootstrapServers = "cluster:9092",
            DefaultSecurityProfile = "default-sec",
            ConfluentOptions =
            {
              ["linger.ms"] = "10"
            }
          }
        },
        SecurityProfiles =
        {
          ["default-sec"] = new KafkaSecuritySettings
          {
            Mode = KafkaSecurityMode.None
          }
        },
        Profiles =
        {
          ["ProducerA"] = new KafkaProfileSettings
          {
            Type = KafkaClientType.Producer,
            Cluster = "primary",
            Topics = { "topic-a" }
          }
        }
      };

      var securityProvider = new CapturingSecurityProvider();
      var factory = CreateFactory(options, securityProvider: securityProvider);

      var runtime = factory.CreateProducer("ProducerA");

      runtime.ConfluentConfig.BootstrapServers.Should().Be("cluster:9092");
      var linger = runtime.ConfluentConfig.Single(kvp => kvp.Key == "linger.ms").Value;
      linger.Should().Be("10");
      securityProvider.LastProfile.Should().Be("ProducerA");
      securityProvider.LastSecurity.Should().BeSameAs(options.SecurityProfiles["default-sec"]);
    }

    [Fact]
    public void CreateConsumer_UsesProfileGroupIdTemplate()
    {
      var options = new KafkaConfigurationRootOptions
      {
        Defaults = new KafkaDefaultsOptions
        {
          GroupIdTemplate = "default-{ProfileName}"
        },
        Clusters =
        {
          ["primary"] = new KafkaClusterSettings
          {
            BootstrapServers = "cluster:9092"
          }
        },
        SecurityProfiles =
        {
          ["none"] = new KafkaSecuritySettings { Mode = KafkaSecurityMode.None }
        },
        Profiles =
        {
          ["ConsumerA"] = new KafkaProfileSettings
          {
            Type = KafkaClientType.Consumer,
            Cluster = "primary",
            SecurityProfile = "none",
            GroupIdTemplate = "profile-{ProfileName}",
            Topics = { "topic-1" }
          }
        }
      };

      var generator = new RecordingGroupIdGenerator();
      var factory = CreateFactory(options, groupIdGenerator: generator);

      var runtime = factory.CreateConsumer("ConsumerA");

      runtime.ConfluentConfig.GroupId.Should().Be("generated-ConsumerA");
      generator.LastTemplate.Should().Be("profile-{ProfileName}");
      generator.LastProfile.Should().Be("ConsumerA");
      runtime.Topics.Should().ContainSingle("topic-1");
    }

    private static KafkaClientConfigFactory CreateFactory(
      KafkaConfigurationRootOptions options,
      IKafkaGroupIdGenerator? groupIdGenerator = null,
      IKafkaSecurityProvider? securityProvider = null,
      IKafkaConfigValidator? validator = null)
    {
      var monitor = new StaticOptionsMonitor<KafkaConfigurationRootOptions>(options);
      return new KafkaClientConfigFactory(
        monitor,
        groupIdGenerator ?? new DefaultKafkaGroupIdGenerator(),
        securityProvider ?? new NoOpSecurityProvider(),
        validator ?? new KafkaConfigValidator(),
        NullLogger<KafkaClientConfigFactory>.Instance);
    }

    private sealed class CapturingSecurityProvider : IKafkaSecurityProvider
    {
      public string? LastProfile { get; private set; }
      public KafkaSecuritySettings? LastSecurity { get; private set; }

      public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
      {
        LastProfile = profileName;
        LastSecurity = security;
      }
    }

    private sealed class NoOpSecurityProvider : IKafkaSecurityProvider
    {
      public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
      {
      }
    }

    private sealed class RecordingGroupIdGenerator : IKafkaGroupIdGenerator
    {
      public string? LastTemplate { get; private set; }
      public string? LastProfile { get; private set; }

      public string GenerateGroupId(string template, string profileName)
      {
        LastTemplate = template;
        LastProfile = profileName;
        return $"generated-{profileName}";
      }
    }

    private sealed class StaticOptionsMonitor<T> : IOptionsMonitor<T>
    {
      public StaticOptionsMonitor(T value)
      {
        CurrentValue = value;
      }

      public T CurrentValue { get; }

      public T Get(string? name) => CurrentValue;

      public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;

      private sealed class NullDisposable : IDisposable
      {
        public static readonly NullDisposable Instance = new();
        public void Dispose()
        {
        }
      }
    }
}
