using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using KF.Kafka.AdminClient.Caching;
using KF.Kafka.AdminClient.Instrumentation;
using KF.Kafka.AdminClient.Models;
using KF.Kafka.Configuration.Admin;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;
using KafkaAdminClientImpl = KF.Kafka.AdminClient.Internal.KafkaAdminClient;
using LagCacheKey = KF.Kafka.AdminClient.Internal.LagCacheKey;
using IKafkaConsumerFactory = KF.Kafka.AdminClient.Internal.IKafkaConsumerFactory;
using TopicMetadataModel = KF.Kafka.AdminClient.Models.TopicMetadata;

namespace KF.Kafka.AdminClient.Tests;

public sealed class KafkaAdminClientTests
{
    [Fact]
    public async Task GetOffsetsForTimestampAsync_UsesConsumerFactoryAndSkipsUnsetOffsets()
    {
        var options = new KafkaAdminOptions { BootstrapServers = "localhost:9092" };
        var metadataCache = CreateMetadataCache();
        metadataCache.Set("orders", new TopicMetadataModel
        {
            TopicName = "orders",
            Exists = true,
            PartitionCount = 2,
            ReplicationFactor = 3
        });
        var lagCache = CreateLagCache();

        var adminClient = Substitute.For<IAdminClient>();
        var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        var consumerFactory = new RecordingConsumerFactory(() => consumer);

        var offsets = new List<TopicPartitionOffset>
        {
            new(new TopicPartition("orders", 0), new Offset(100)),
            new(new TopicPartition("orders", 1), Offset.Unset)
        };

        consumer
            .OffsetsForTimes(Arg.Any<IEnumerable<TopicPartitionTimestamp>>(), Arg.Any<TimeSpan>())
            .Returns(offsets);

        var scope = Substitute.For<IKafkaAdminCallScope>();
        var metrics = Substitute.For<IKafkaAdminMetrics>();
        metrics.TrackCall("offsets-for-timestamp", "orders", null).Returns(scope);

        await using var sut = CreateSut(options, metrics, adminClient, consumerFactory, metadataCache, lagCache);
        var timestamp = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        var result = await sut.GetOffsetsForTimestampAsync("orders", timestamp);

        result.Should().HaveCount(1);
        result[new TopicPartition("orders", 0)].Should().Be(100);

        consumerFactory.CreateCount.Should().Be(1);
        consumer.Received(1).OffsetsForTimes(
            Arg.Is<IEnumerable<TopicPartitionTimestamp>>(specs => specs.Count() == 2),
            Arg.Any<TimeSpan>());
        consumer.Received(1).Close();
        metrics.Received(1).TrackCall("offsets-for-timestamp", "orders", null);
        scope.Received(1).Dispose();
    }

    [Fact]
    public async Task GetConsumerGroupLagAsync_ReturnsCachedEntries()
    {
        var options = new KafkaAdminOptions { BootstrapServers = "localhost:9092" };
        var metadataCache = CreateMetadataCache();
        var lagCache = CreateLagCache();

        var adminClient = Substitute.For<IAdminClient>();
        var consumerFactory = new RecordingConsumerFactory(() => throw new InvalidOperationException("Factory should not be invoked for cache hits."));
        var metrics = Substitute.For<IKafkaAdminMetrics>();

        var query = new ConsumerGroupLagQuery
        {
            TopicFilter = new[] { "orders", "payments" },
            ExcludeEmptyTopics = true
        };

        var cachedLag = new List<ConsumerGroupLag>
        {
            new()
            {
                ConsumerGroupId = "group",
                TopicName = "orders",
                Partition = 0,
                CommittedOffset = 10,
                EndOffset = 20
            }
        };

        var key = LagCacheKey.From("group", query);
        lagCache.Set(key, cachedLag);

        await using var sut = CreateSut(options, metrics, adminClient, consumerFactory, metadataCache, lagCache);

        var result = await sut.GetConsumerGroupLagAsync("group", query);

        result.Should().BeSameAs(cachedLag);
        _ = adminClient
            .DidNotReceiveWithAnyArgs()
            .ListConsumerGroupOffsetsAsync(default!, default);
        consumerFactory.CreateCount.Should().Be(0);
    }

    [Fact]
    public async Task GetTopicMetadataAsync_RetriesUntilMaxOnKafkaException()
    {
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "localhost:9092",
            Retry = new KafkaAdminRetryOptions
            {
                MaxRetries = 2,
                InitialBackoffMs = 1,
                MaxBackoffMs = 1
            }
        };

        var metadataCache = CreateMetadataCache();
        var lagCache = CreateLagCache();

        var adminClient = Substitute.For<IAdminClient>();
        adminClient
            .GetMetadata("orders", Arg.Any<TimeSpan>())
            .Returns(_ => throw new KafkaException(new Error(ErrorCode.LeaderNotAvailable)));

        var consumerFactory = new RecordingConsumerFactory(() => Substitute.For<IConsumer<byte[], byte[]>>());
        var metrics = new RecordingMetrics();

        await using var sut = CreateSut(options, metrics, adminClient, consumerFactory, metadataCache, lagCache);

        var action = () => sut.GetTopicMetadataAsync("orders");

        await action.Should().ThrowAsync<KafkaException>();

        adminClient.Received(3).GetMetadata("orders", Arg.Any<TimeSpan>());
        metrics.TrackCallCount.Should().Be(3);
        metrics.ScopeErrorCount.Should().Be(3);
    }

    [Fact]
    public async Task GetTopicMetadataAsync_RetriesOnceThenCachesResult()
    {
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "localhost:9092",
            Retry = new KafkaAdminRetryOptions
            {
                MaxRetries = 2,
                InitialBackoffMs = 1,
                MaxBackoffMs = 1
            }
        };

        var metadataCache = CreateMetadataCache();
        var lagCache = CreateLagCache();

        var adminClient = Substitute.For<IAdminClient>();
        var topicMetadata = new Confluent.Kafka.TopicMetadata(
            "orders",
            new List<PartitionMetadata>
            {
                new(0, 1, new[] { 1, 2 }, new[] { 1, 2 }, new Error(ErrorCode.NoError)),
                new(1, 2, new[] { 2, 3 }, new[] { 2, 3 }, new Error(ErrorCode.NoError))
            },
            new Error(ErrorCode.NoError));

        var metadata = new Metadata(
            new List<BrokerMetadata> { new(1, "localhost", 9092) },
            new List<Confluent.Kafka.TopicMetadata> { topicMetadata },
            1,
            "broker-1");

        adminClient
            .GetMetadata("orders", Arg.Any<TimeSpan>())
            .Returns(
                _ => throw new KafkaException(new Error(ErrorCode.LeaderNotAvailable)),
                _ => metadata);

        var consumerFactory = new RecordingConsumerFactory(() => Substitute.For<IConsumer<byte[], byte[]>>());
        var metrics = new RecordingMetrics();

        await using var sut = CreateSut(options, metrics, adminClient, consumerFactory, metadataCache, lagCache);

        var first = await sut.GetTopicMetadataAsync("orders");
        var second = await sut.GetTopicMetadataAsync("orders");

        first.TopicName.Should().Be("orders");
        first.PartitionCount.Should().Be(2);
        first.ReplicationFactor.Should().Be(2);
        second.Should().BeSameAs(first);

        adminClient.Received(2).GetMetadata("orders", Arg.Any<TimeSpan>());
        metrics.TrackCallCount.Should().Be(2);
        metrics.ScopeErrorCount.Should().Be(1);
    }

    [Fact]
    public async Task GetConsumerGroupLagAsync_RetriesAndCachesResult()
    {
        var options = new KafkaAdminOptions
        {
            BootstrapServers = "localhost:9092",
            MaxDegreeOfParallelism = 1,
            Retry = new KafkaAdminRetryOptions
            {
                MaxRetries = 2,
                InitialBackoffMs = 1,
                MaxBackoffMs = 1
            }
        };

        var metadataCache = CreateMetadataCache();
        var lagCache = CreateLagCache();

        var adminClient = Substitute.For<IAdminClient>();
        var partitions = new List<TopicPartitionOffsetError>
        {
            new("orders", 0, 5, ErrorCode.NoError),
            new("orders", 1, 8, ErrorCode.NoError)
        };

        var lagResponse = new List<ListConsumerGroupOffsetsResult>
        {
            new()
            {
                Group = "group-1",
                Partitions = partitions
            }
        };

        var retriable = new KafkaException(new Error(ErrorCode.RequestTimedOut));
        adminClient
            .ListConsumerGroupOffsetsAsync(
                Arg.Any<IEnumerable<ConsumerGroupTopicPartitions>>(),
                Arg.Any<ListConsumerGroupOffsetsOptions>())
            .Returns(
                _ => Task.FromException<List<ListConsumerGroupOffsetsResult>>(retriable),
                _ => Task.FromResult(lagResponse));

        var consumer = Substitute.For<IConsumer<byte[], byte[]>>();
        consumer
            .QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>())
            .Returns(callInfo => new WatermarkOffsets(0, 100 + callInfo.Arg<TopicPartition>().Partition.Value));

        var consumerFactory = new RecordingConsumerFactory(() => consumer);
        var metrics = new RecordingMetrics();

        await using var sut = CreateSut(options, metrics, adminClient, consumerFactory, metadataCache, lagCache);

        var query = new ConsumerGroupLagQuery();
        var first = await sut.GetConsumerGroupLagAsync("group-1", query);
        var second = await sut.GetConsumerGroupLagAsync("group-1", query);

        first.Should().HaveCount(2);
        first.Select(l => l.TopicName).Should().OnlyContain(name => name == "orders");
        first.Select(l => l.ConsumerGroupId).Should().OnlyContain(id => id == "group-1");
        second.Should().BeSameAs(first);

        _ = adminClient.Received(2).ListConsumerGroupOffsetsAsync(
            Arg.Any<IEnumerable<ConsumerGroupTopicPartitions>>(),
            Arg.Any<ListConsumerGroupOffsetsOptions>());
        consumerFactory.CreateCount.Should().Be(2);
        consumer.Received(2).QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>());
        consumer.Received(2).Close();
        metrics.TrackCallCount.Should().Be(2);
        metrics.ScopeErrorCount.Should().Be(1);
    }

    private static KafkaAdminClientImpl CreateSut(
        KafkaAdminOptions options,
        IKafkaAdminMetrics metrics,
        IAdminClient adminClient,
        IKafkaConsumerFactory consumerFactory,
        KafkaAdminCache<string, TopicMetadataModel> metadataCache,
        KafkaAdminCache<LagCacheKey, IReadOnlyList<ConsumerGroupLag>> lagCache)
    {
        return new KafkaAdminClientImpl(
            Options.Create(options),
            NullLogger<KafkaAdminClientImpl>.Instance,
            metrics,
            adminClient,
            consumerFactory,
            metadataCache,
            lagCache);
    }

    private static KafkaAdminCache<string, TopicMetadataModel> CreateMetadataCache() =>
        new(
            "metadata-test",
            enabled: true,
            TimeSpan.FromMinutes(5),
            NoOpKafkaAdminMetrics.Instance);

    private static KafkaAdminCache<LagCacheKey, IReadOnlyList<ConsumerGroupLag>> CreateLagCache() =>
        new(
            "lag-test",
            enabled: true,
            TimeSpan.FromMinutes(5),
            NoOpKafkaAdminMetrics.Instance);

    private sealed class RecordingConsumerFactory : IKafkaConsumerFactory
    {
        private readonly Func<IConsumer<byte[], byte[]>> _create;

        public RecordingConsumerFactory(Func<IConsumer<byte[], byte[]>> create)
        {
            _create = create;
        }

        public int CreateCount { get; private set; }

        public IConsumer<byte[], byte[]> CreateConsumer()
        {
            CreateCount++;
            return _create();
        }
    }

    private sealed class RecordingMetrics : IKafkaAdminMetrics
    {
        public int TrackCallCount { get; private set; }
        public int ScopeErrorCount { get; private set; }

        public void RecordCacheHit(string cacheName)
        {
        }

        public void RecordCacheMiss(string cacheName)
        {
        }

        public IKafkaAdminCallScope TrackCall(string operation, string? topic, string? groupId) => new Scope(this);

        private sealed class Scope : IKafkaAdminCallScope
        {
            private readonly RecordingMetrics _metrics;

            public Scope(RecordingMetrics metrics)
            {
                _metrics = metrics;
                _metrics.TrackCallCount++;
            }

            public void MarkError(Exception exception)
            {
                _metrics.ScopeErrorCount++;
            }

            public void Dispose()
            {
            }
        }
    }
}
