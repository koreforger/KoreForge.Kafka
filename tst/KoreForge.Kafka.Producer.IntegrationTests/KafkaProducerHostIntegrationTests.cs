using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Configuration.Generation;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Configuration.Runtime;
using KoreForge.Kafka.Configuration.Security;
using KoreForge.Kafka.Configuration.Validation;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Hosting;
using KoreForge.Kafka.Producer.Serialization;
using KoreForge.Kafka.Producer.IntegrationTests.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KoreForge.Kafka.Producer.IntegrationTests;

[Collection(KafkaClusterCollection.CollectionName)]
public sealed class KafkaProducerHostIntegrationTests
{
    private readonly KafkaTestClusterFixture _fixture;
    private readonly JsonSerializerOptions _serializerOptions = new(JsonSerializerDefaults.Web);

    public KafkaProducerHostIntegrationTests(KafkaTestClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Host_publishes_messages_to_kafka()
    {
        var topic = $"producer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);

        await using var host = BuildHost(topic);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        await host.StartAsync(cts.Token);

        var expected = Enumerable.Range(0, 6)
            .Select(i => new TestMessage(i, $"payload-{i}"))
            .ToArray();

        foreach (var message in expected)
        {
            await host.Buffer.EnqueueAsync(topic, message, cancellationToken: cts.Token);
        }

        var records = await ConsumeMessagesAsync(topic, expected.Length, TimeSpan.FromSeconds(30));
        await host.StopAsync(cts.Token);

        records.Select(r => r.Payload.Value).OrderBy(v => v).Should().Equal(expected.Select(m => m.Value));
        records.Should().OnlyContain(r => r.Headers.Count == 0 && r.Key == null);
    }

    [Fact]
    public async Task Host_respects_keys_and_headers_when_publishing()
    {
        var topic = $"producer-headers-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);

        await using var host = BuildHost(topic);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await host.StartAsync(cts.Token);

        var enqueueOptions = new ProducerEnqueueOptions
        {
            Key = "order-42",
            Headers = new Dictionary<string, string>
            {
                ["trace-id"] = Guid.NewGuid().ToString("N"),
                ["tenant"] = "integration"
            }
        };

        var message = new TestMessage(42, "important");
        await host.Buffer.EnqueueAsync(topic, message, enqueueOptions, cts.Token);

        var records = await ConsumeMessagesAsync(topic, expectedCount: 1, timeout: TimeSpan.FromSeconds(30));
        await host.StopAsync(cts.Token);

        var record = records.Single();
        record.Payload.Should().BeEquivalentTo(message);
        record.Key.Should().Be(enqueueOptions.Key);
        record.Headers.Should().ContainKey("trace-id");
        record.Headers["tenant"].Should().Be("integration");
    }

    private KafkaProducerHost BuildHost(
        string topic,
        ExtendedProducerSettings? extendedSettings = null,
        ILoggerFactory? loggerFactory = null)
    {
        var profileName = $"producer-profile-{Guid.NewGuid():N}";
        var root = new KafkaConfigurationRootOptions();
        root.Clusters["primary"] = new KafkaClusterSettings
        {
            BootstrapServers = _fixture.BootstrapServers
        };

        var settings = extendedSettings ?? new ExtendedProducerSettings
        {
            TopicDefaults = new ProducerTopicDefaults
            {
                WorkerCount = 1,
                ChannelCapacity = 32
            },
            Backlog = new ProducerBacklogSettings
            {
                PersistenceEnabled = false,
                ReplayEnabled = false,
                Directory = Path.Combine(Path.GetTempPath(), "kafka-producer-int-tests")
            },
            Alerts = new ProducerAlertSettings
            {
                Enabled = false,
                EvaluationInterval = TimeSpan.FromSeconds(5)
            }
        };

        var profile = new KafkaProfileSettings
        {
            Type = KafkaClientType.Producer,
            Cluster = "primary",
            ExtendedProducer = settings
        };

        profile.Topics.Add(topic);
        profile.ConfluentOptions["acks"] = "all";
        profile.ConfluentOptions["client.id"] = profileName;
        root.Profiles[profileName] = profile;

        var factory = new KafkaClientConfigFactory(
            new StaticOptionsMonitor<KafkaConfigurationRootOptions>(root),
            new DefaultKafkaGroupIdGenerator(),
            new NoOpSecurityProvider(),
            new KafkaConfigValidator(),
            NullLogger<KafkaClientConfigFactory>.Instance);

        return KafkaProducerHost.Create()
            .UseKafkaConfigurationProfile(profileName, factory)
            .UseJsonSerializer<TestMessage>()
            .UseLoggerFactory(loggerFactory ?? NullLoggerFactory.Instance)
            .Build();
    }

    private async Task<IReadOnlyList<ConsumedRecord>> ConsumeMessagesAsync(string topic, int expectedCount, TimeSpan timeout)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _fixture.BootstrapServers,
            GroupId = $"verification-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(Deserializers.Utf8)
            .Build();

        consumer.Subscribe(topic);

        var deadline = DateTime.UtcNow + timeout;
        var records = new List<ConsumedRecord>();

        try
        {
            while (records.Count < expectedCount && DateTime.UtcNow < deadline)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
                if (result is null)
                {
                    continue;
                }

                var payload = JsonSerializer.Deserialize<TestMessage>(result.Message.Value, _serializerOptions)!;
                var headers = ReadHeaders(result.Message.Headers);
                records.Add(new ConsumedRecord(payload, result.Message.Key, headers));
            }
        }
        finally
        {
            consumer.Close();
        }

        if (records.Count < expectedCount)
        {
            throw new TimeoutException($"Timed out waiting for {expectedCount} messages. Received {records.Count}.");
        }

        return records;
    }

    private static IReadOnlyDictionary<string, string> ReadHeaders(Headers? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return new Dictionary<string, string>();
        }

        var map = new Dictionary<string, string>(headers.Count, StringComparer.Ordinal);
        foreach (var header in headers)
        {
            map[header.Key] = header.GetValueBytes() is { Length: > 0 } bytes
                ? Encoding.UTF8.GetString(bytes)
                : string.Empty;
        }

        return map;
    }

    private sealed record TestMessage(int Value, string? Note);

    private sealed record ConsumedRecord(TestMessage Payload, string? Key, IReadOnlyDictionary<string, string> Headers);

    private sealed class StaticOptionsMonitor<T> : Microsoft.Extensions.Options.IOptionsMonitor<T> where T : class
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

    private sealed class NoOpSecurityProvider : IKafkaSecurityProvider
    {
        public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
        {
            // No security adjustments required for plaintext integration cluster.
        }
    }
}
