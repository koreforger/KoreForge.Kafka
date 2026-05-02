using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.AdminClient.Models;
using KoreForge.Kafka.Configuration.Admin;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;
using System.Linq;
using KafkaAdminClientImpl = KoreForge.Kafka.AdminClient.Internal.KafkaAdminClient;

namespace KoreForge.Kafka.AdminClient.IntegrationTests;

[Collection(KafkaClusterCollection.CollectionName)]
public sealed class AdminClientIntegrationTests
{
    private readonly KafkaTestClusterFixture _fixture;

    public AdminClientIntegrationTests(KafkaTestClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task GetTopicMetadataAsync_ReturnsActualPartitions()
    {
        var topic = $"metadata-topic-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 3);

        await using var sut = CreateSut();
        var metadata = await sut.GetTopicMetadataAsync(topic);

        metadata.Exists.Should().BeTrue();
        metadata.PartitionCount.Should().Be(3);
        metadata.ReplicationFactor.Should().Be(1);
    }

    [Fact]
    public async Task GetConsumerGroupLagAsync_ComputesLagFromKafka()
    {
        var topic = $"lag-topic-{Guid.NewGuid():N}";
        var groupId = $"group-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 1);
        await _fixture.ProduceAsync(topic, partition: 0, messageCount: 10);
        await _fixture.CommitOffsetAsync(groupId, topic, partition: 0, offset: 4);

        var query = new ConsumerGroupLagQuery
        {
            TopicFilter = new[] { topic }
        };

        await using var sut = CreateSut();
        var result = await sut.GetConsumerGroupLagAsync(groupId, query);

        result.Should().ContainSingle();
        var lag = result.Single();
        lag.TopicName.Should().Be(topic);
        lag.Partition.Should().Be(0);
        lag.CommittedOffset.Should().Be(4);
        lag.EndOffset.Should().Be(10);
        lag.Lag.Should().Be(6);
    }

    [Fact]
    public async Task GetOffsetsForTimestampAsync_ReturnsOffsetsPerPartition()
    {
        var topic = $"timestamp-topic-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 2);
        var timestamp = DateTime.UtcNow.AddSeconds(-5);

        await _fixture.ProduceAsync(topic, 0, 1, timestamp);
        await _fixture.ProduceAsync(topic, 1, 1, timestamp);

        await using var sut = CreateSut();
        var offsets = await sut.GetOffsetsForTimestampAsync(topic, timestamp);

        offsets.Should().ContainKey(new TopicPartition(topic, 0));
        offsets.Should().ContainKey(new TopicPartition(topic, 1));
        offsets[new TopicPartition(topic, 0)].Should().Be(0);
        offsets[new TopicPartition(topic, 1)].Should().Be(0);
    }

    private KafkaAdminClientImpl CreateSut()
    {
        var options = new KafkaAdminOptions
        {
            BootstrapServers = _fixture.BootstrapServers,
            RequestTimeoutMs = 5_000,
            MaxDegreeOfParallelism = 2,
            Cache = new KafkaAdminCacheOptions
            {
                LagCacheEnabled = false,
                MetadataCacheEnabled = false,
                LagCacheTtlMs = 10,
                MetadataCacheTtlMs = 10
            },
            Retry = new KafkaAdminRetryOptions
            {
                MaxRetries = 3,
                InitialBackoffMs = 100,
                MaxBackoffMs = 500
            }
        };

        return new KafkaAdminClientImpl(
            Options.Create(options),
            NullLogger<KafkaAdminClientImpl>.Instance);
    }
}
