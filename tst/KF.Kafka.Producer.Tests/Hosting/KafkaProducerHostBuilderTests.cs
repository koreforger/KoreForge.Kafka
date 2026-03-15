using System;
using System;
using System.Text;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Backlog;
using KF.Kafka.Producer.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KF.Kafka.Producer.Tests.Hosting;

public sealed class KafkaProducerHostBuilderTests
{
    [Fact]
    public void Build_UsesCustomBacklogStrategyFactory_WhenProvided()
    {
        var runtimeConfig = CreateRuntimeConfig();
        var factory = new TestKafkaClientConfigFactory(runtimeConfig);
        var backlogFactory = new TestBacklogStrategyFactory();

        KafkaProducerHost
            .Create()
            .UseKafkaConfigurationProfile("test", factory)
            .UseLoggerFactory(NullLoggerFactory.Instance)
            .UseBacklogPersistenceFactory(backlogFactory)
            .Build();

        backlogFactory.Invocations.Should().Be(1);
    }

    [Fact]
    public void UseSerializerRegistrations_LoadsRegistrationsFromServiceProvider()
    {
        var services = new ServiceCollection();
        var registration = new TestSerializerRegistration();
        services.AddSingleton<IProducerSerializerRegistration>(registration);
        var provider = services.BuildServiceProvider();

        KafkaProducerHost
            .Create()
            .UseKafkaConfigurationProfile("test", new TestKafkaClientConfigFactory(CreateRuntimeConfig()))
            .UseLoggerFactory(NullLoggerFactory.Instance)
            .UseSerializerRegistrations(provider);

        registration.InvocationCount.Should().Be(1);
    }

    private static KafkaProducerRuntimeConfig CreateRuntimeConfig()
    {
        var producerConfig = new ProducerConfig { BootstrapServers = "test:9092" };
        var extended = new ExtendedProducerSettings();
        return new KafkaProducerRuntimeConfig(producerConfig, extended, new[] { "topic-a" });
    }

    private sealed class TestKafkaClientConfigFactory : IKafkaClientConfigFactory
    {
        private readonly KafkaProducerRuntimeConfig _runtimeConfig;

        public TestKafkaClientConfigFactory(KafkaProducerRuntimeConfig runtimeConfig)
        {
            _runtimeConfig = runtimeConfig;
        }

        public KafkaConsumerRuntimeConfig CreateConsumer(string profileName)
            => throw new NotSupportedException();

        public KafkaProducerRuntimeConfig CreateProducer(string profileName) => _runtimeConfig;

        public KafkaAdminRuntimeConfig CreateAdmin(string profileName)
            => throw new NotSupportedException();
    }

    private sealed class TestBacklogStrategyFactory : IProducerBacklogPersistenceStrategyFactory
    {
        public int Invocations { get; private set; }

        public IProducerBacklogPersistenceStrategy Create(ExtendedProducerSettings settings, ILoggerFactory loggerFactory)
        {
            Invocations++;
            return NoOpProducerBacklogPersistenceStrategy.Instance;
        }
    }

    private sealed class TestSerializerRegistration : IProducerSerializerRegistration
    {
        public int InvocationCount { get; private set; }

        public void Register(IProducerSerializerRegistry registry, IServiceProvider serviceProvider)
        {
            InvocationCount++;
            registry.RegisterSerializer(new TestSerializer());
        }
    }

    private sealed class TestSerializer : IProducerSerializer<TestPayload>
    {
        public SerializedRecord Serialize(TestPayload message, OutgoingEnvelope envelope)
        {
            return new SerializedRecord
            {
                Topic = envelope.Topic,
                ValueBytes = System.Text.Encoding.UTF8.GetBytes(message.Value)
            };
        }
    }

    private sealed record TestPayload(string Value);
}
