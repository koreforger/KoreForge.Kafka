using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;
using Xunit;

namespace KF.Kafka.Consumer.IntegrationTests.Infrastructure;

public sealed class KafkaTestClusterFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer;

    public KafkaTestClusterFixture()
    {
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.6.1")
            .Build();
    }

    public string BootstrapServers => _kafkaContainer.GetBootstrapAddress();

    public async Task InitializeAsync()
    {
        await _kafkaContainer.StartAsync().ConfigureAwait(false);
        await WaitUntilReadyAsync().ConfigureAwait(false);
    }

    public Task DisposeAsync() => _kafkaContainer.DisposeAsync().AsTask();

    public async Task CreateTopicAsync(string topicName, int partitions = 1)
    {
        using var admin = BuildAdminClient();
        try
        {
            await admin.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            }).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Topic already created by a previous test; safe to ignore.
        }
    }

    public async Task ProduceAsync(string topicName, int partition, int messageCount, DateTime? timestampUtc = null)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
        };

        using var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
        for (var i = 0; i < messageCount; i++)
        {
            var message = new Message<byte[], byte[]>
            {
                Value = BitConverter.GetBytes(i),
                Timestamp = timestampUtc.HasValue ? new Timestamp(timestampUtc.Value) : Timestamp.Default
            };

            await producer.ProduceAsync(new TopicPartition(topicName, partition), message).ConfigureAwait(false);
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    public async Task CommitOffsetAsync(string groupId, string topicName, int partition, long offset)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
        var tpo = new TopicPartitionOffset(topicName, partition, new Offset(offset));
        consumer.Assign(tpo.TopicPartition);
        consumer.Commit(new[] { tpo });
        consumer.Close();
        await Task.CompletedTask;
    }

    private IAdminClient BuildAdminClient()
    {
        var config = new AdminClientConfig { BootstrapServers = BootstrapServers };
        return new AdminClientBuilder(config).Build();
    }

    private async Task WaitUntilReadyAsync()
    {
        using var admin = BuildAdminClient();
        var deadline = DateTime.UtcNow.AddMinutes(1);

        while (true)
        {
            try
            {
                _ = admin.GetMetadata(TimeSpan.FromSeconds(2));
                return;
            }
            catch
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
        }
    }
}

[CollectionDefinition(CollectionName)]
public sealed class KafkaClusterCollection : ICollectionFixture<KafkaTestClusterFixture>
{
    public const string CollectionName = "kafka-test-cluster";
}
