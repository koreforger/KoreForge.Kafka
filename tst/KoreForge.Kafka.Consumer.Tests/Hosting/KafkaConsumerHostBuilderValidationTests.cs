using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Exceptions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Configuration.Generation;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Security;
using KoreForge.Kafka.Configuration.Validation;
using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Kafka.Consumer.Hosting;
using KoreForge.Kafka.Consumer.Pipelines;
using KoreForge.Processing.Pipelines;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Hosting;

public sealed class KafkaConsumerHostBuilderValidationTests
{
    [Fact]
    public void Build_throws_when_topics_missing()
    {
        var builder = CreateBuilder(profile => profile.Topics.Clear());

        var act = () => builder.Build();

        act.Should().Throw<KafkaProfileValidationException>()
            .WithMessage("*topic*");
    }

    [Fact]
    public void Build_throws_when_max_batch_size_invalid()
    {
        var builder = CreateBuilder(profile =>
        {
            profile.ExtendedConsumer = new ExtendedConsumerSettings
            {
                ConsumerCount = 1,
                MaxBatchSize = 0,
                MaxBatchWaitMs = 500,
                StartMode = StartMode.Earliest
            };
        });

        var act = () => builder.Build();

        act.Should().Throw<KafkaProfileValidationException>()
            .WithMessage("*MaxBatchSize*");
    }

    [Fact]
    public void Build_throws_when_max_batch_wait_invalid()
    {
        var builder = CreateBuilder(profile =>
        {
            profile.ExtendedConsumer = new ExtendedConsumerSettings
            {
                ConsumerCount = 1,
                MaxBatchSize = 10,
                MaxBatchWaitMs = 0,
                StartMode = StartMode.Earliest
            };
        });

        var act = () => builder.Build();

        act.Should().Throw<KafkaProfileValidationException>()
            .WithMessage("*MaxBatchWaitMs*");
    }

    [Fact]
    public async Task Build_succeeds_with_processing_pipeline()
    {
        var builder = CreateBuilder(
            configureProcessor: b => b.UseProcessingPipeline<KafkaPipelineRecord>(() => global::KoreForge.Processing.Pipelines.Pipeline.Start<KafkaPipelineRecord>().Build()));

        await using var host = builder.Build();
        host.Should().NotBeNull();
    }

    [Fact]
    public void Build_throws_when_backpressure_thresholds_invalid()
    {
        var builder = CreateBuilder(profile =>
        {
            profile.ExtendedConsumer = new ExtendedConsumerSettings
            {
                ConsumerCount = 1,
                MaxBatchSize = 10,
                MaxBatchWaitMs = 500,
                StartMode = StartMode.Earliest,
                Backpressure = new ConsumerBackpressureSettings
                {
                    PauseUtilization = 0.5,
                    ResumeUtilization = 0.6
                }
            };
        });

        var act = () => builder.Build();

        act.Should().Throw<ArgumentException>()
            .WithMessage("*ResumeUtilization*");
    }

    private static KafkaConsumerHostBuilder CreateBuilder(
        Action<KafkaProfileSettings>? configureProfile = null,
        Func<KafkaConsumerHostBuilder, KafkaConsumerHostBuilder>? configureProcessor = null)
    {
        var profileName = $"test-{Guid.NewGuid():N}";
        var root = new KafkaConfigurationRootOptions();
        root.Clusters["primary"] = new KafkaClusterSettings
        {
            BootstrapServers = "localhost:9092"
        };

        var profile = new KafkaProfileSettings
        {
            Type = KafkaClientType.Consumer,
            Cluster = "primary",
            ExplicitGroupId = "group",
            Topics = new List<string> { "topic-1" },
            ExtendedConsumer = new ExtendedConsumerSettings
            {
                ConsumerCount = 1,
                MaxBatchSize = 10,
                MaxBatchWaitMs = 500,
                StartMode = StartMode.Earliest
            }
        };

        configureProfile?.Invoke(profile);
        root.Profiles[profileName] = profile;

        var factory = new KafkaClientConfigFactory(
            new StaticOptionsMonitor<KafkaConfigurationRootOptions>(root),
            new DefaultKafkaGroupIdGenerator(),
            new NoOpSecurityProvider(),
            new KafkaConfigValidator(),
            NullLogger<KafkaClientConfigFactory>.Instance);

        var builder = KafkaConsumerHost.Create()
            .UseKafkaConfigurationProfile(profileName, factory);

        return configureProcessor?.Invoke(builder) ?? builder.UseProcessor<NoOpProcessor>();
    }

    private sealed class NoOpProcessor : IKafkaBatchProcessor
    {
        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class NoOpSecurityProvider : IKafkaSecurityProvider
    {
        public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
        {
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
