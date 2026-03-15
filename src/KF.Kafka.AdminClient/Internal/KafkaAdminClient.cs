using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KF.Kafka.AdminClient.Abstractions;
using KF.Kafka.AdminClient.Caching;
using KF.Kafka.AdminClient.Instrumentation;
using KF.Kafka.AdminClient.Models;
using KF.Kafka.Configuration.Admin;
using KF.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TopicMetadataModel = KF.Kafka.AdminClient.Models.TopicMetadata;

namespace KF.Kafka.AdminClient.Internal;

internal sealed class KafkaAdminClient : IKafkaAdminClient
{
    private const string MetadataOperation = "metadata";
    private const string LagOperation = "lag";
    private const string OffsetTimestampOperation = "offsets-for-timestamp";

    private readonly IAdminClient _adminClient;
    private readonly KafkaAdminOptions _options;
    private readonly IKafkaAdminMetrics _metrics;
    private readonly ILogger<KafkaAdminClient> _logger;
    private readonly IKafkaConsumerFactory _consumerFactory;
    private readonly KafkaAdminCache<string, TopicMetadataModel> _metadataCache;
    private readonly KafkaAdminCache<LagCacheKey, IReadOnlyList<ConsumerGroupLag>> _lagCache;
    private readonly ISystemClock _clock;

    public KafkaAdminClient(
        IOptions<KafkaAdminOptions> options,
        ILogger<KafkaAdminClient> logger,
        IKafkaAdminMetrics? metrics = null,
        IAdminClient? adminClient = null,
        IKafkaConsumerFactory? consumerFactory = null,
        KafkaAdminCache<string, TopicMetadataModel>? metadataCache = null,
        KafkaAdminCache<LagCacheKey, IReadOnlyList<ConsumerGroupLag>>? lagCache = null,
        AdminClientConfig? adminClientConfig = null,
        ISystemClock? clock = null)
    {
        _options = options.Value;
        _logger = logger;
        _metrics = metrics ?? NoOpKafkaAdminMetrics.Instance;
        _consumerFactory = consumerFactory ?? new KafkaConsumerFactory(_options);
        _clock = clock ?? SystemClock.Instance;

        var effectiveConfig = adminClientConfig is null
            ? BuildAdminConfig(_options)
            : new AdminClientConfig(adminClientConfig);

        _adminClient = adminClient ?? new AdminClientBuilder(effectiveConfig).Build();

        _metadataCache = metadataCache ?? new KafkaAdminCache<string, TopicMetadataModel>(
            "metadata",
            _options.Cache.MetadataCacheEnabled,
            TimeSpan.FromMilliseconds(_options.Cache.MetadataCacheTtlMs),
            _metrics,
            _clock);

        _lagCache = lagCache ?? new KafkaAdminCache<LagCacheKey, IReadOnlyList<ConsumerGroupLag>>(
            "lag",
            _options.Cache.LagCacheEnabled,
            TimeSpan.FromMilliseconds(_options.Cache.LagCacheTtlMs),
            _metrics,
            _clock);
    }

    public Task<TopicMetadataModel> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
        {
            throw new ArgumentException("Topic name is required.", nameof(topicName));
        }

        if (_metadataCache.TryGet(topicName, out var cached))
        {
            return Task.FromResult(cached!);
        }

        return ExecuteWithRetryAsync(
            MetadataOperation,
            topicName,
            groupId: null,
            async ct =>
            {
                ct.ThrowIfCancellationRequested();
                var metadata = _adminClient.GetMetadata(topicName, TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));
                var topic = metadata.Topics.SingleOrDefault(t => t.Topic == topicName);
                if (topic == null)
                {
                    return new TopicMetadataModel
                    {
                        TopicName = topicName,
                        Exists = false,
                        PartitionCount = 0,
                        ReplicationFactor = 0
                    };
                }

                if (topic.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    return new TopicMetadataModel
                    {
                        TopicName = topicName,
                        Exists = false,
                        PartitionCount = 0,
                        ReplicationFactor = 0
                    };
                }

                var replication = topic.Partitions.Count == 0
                    ? 0
                    : topic.Partitions.Max(p => p.Replicas.Length);

                var result = new TopicMetadataModel
                {
                    TopicName = topic.Topic,
                    Exists = true,
                    PartitionCount = topic.Partitions.Count,
                    ReplicationFactor = replication
                };

                _metadataCache.Set(topicName, result);
                return result;
            },
            cancellationToken);
    }

    public async Task<IReadOnlyList<ConsumerGroupLag>> GetConsumerGroupLagAsync(
        string consumerGroupId,
        ConsumerGroupLagQuery query,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroupId);
        query ??= new ConsumerGroupLagQuery();

        var cacheKey = LagCacheKey.From(consumerGroupId, query);
        if (_lagCache.TryGet(cacheKey, out var cached))
        {
            return cached!;
        }

        var result = await FetchConsumerGroupLagAsync(consumerGroupId, query, cancellationToken).ConfigureAwait(false);
        _lagCache.Set(cacheKey, result);
        return result;
    }

    public async Task<ConsumerGroupLagSummary> GetConsumerGroupLagSummaryAsync(
        string consumerGroupId,
        ConsumerGroupLagQuery query,
        CancellationToken cancellationToken = default)
    {
        var lagDetails = await GetConsumerGroupLagAsync(consumerGroupId, query, cancellationToken).ConfigureAwait(false);
        return LagSummaryFactory.Create(consumerGroupId, lagDetails, _options.LagHealth);
    }

    public async Task<IReadOnlyDictionary<TopicPartition, long>> GetOffsetsForTimestampAsync(
        string topicName,
        DateTime timestampUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        if (timestampUtc.Kind != DateTimeKind.Utc)
        {
            throw new ArgumentException("Timestamp must be expressed in UTC.", nameof(timestampUtc));
        }

        var topicMetadata = await GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);
        if (!topicMetadata.Exists || topicMetadata.PartitionCount == 0)
        {
            return new Dictionary<TopicPartition, long>();
        }

        return await ExecuteWithRetryAsync(
            OffsetTimestampOperation,
            topicName,
            groupId: null,
            async ct =>
            {
                using var consumer = _consumerFactory.CreateConsumer();
                var timestamp = new Timestamp(timestampUtc);
                var specs = Enumerable.Range(0, topicMetadata.PartitionCount)
                    .Select(partition => new TopicPartitionTimestamp(new TopicPartition(topicName, partition), timestamp))
                    .ToList();

                var offsets = consumer.OffsetsForTimes(specs, TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));
                consumer.Close();

                return offsets
                    .Where(o => o.Offset != Offset.Unset)
                    .ToDictionary(o => o.TopicPartition, o => o.Offset.Value);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        _adminClient.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<IReadOnlyList<ConsumerGroupLag>> FetchConsumerGroupLagAsync(
        string consumerGroupId,
        ConsumerGroupLagQuery query,
        CancellationToken cancellationToken)
    {
        var topicPartitions = query.TopicFilter is null || query.TopicFilter.Count == 0
            ? new List<TopicPartition>()
            : await BuildTopicPartitionsAsync(query.TopicFilter, cancellationToken).ConfigureAwait(false);

        var partitions = await ExecuteWithRetryAsync(
            LagOperation,
            topic: null,
            consumerGroupId,
            async ct =>
            {
                var request = new ConsumerGroupTopicPartitions(consumerGroupId, topicPartitions);
                var options = new ListConsumerGroupOffsetsOptions
                {
                    RequestTimeout = TimeSpan.FromMilliseconds(_options.RequestTimeoutMs)
                };

                var results = await _adminClient
                    .ListConsumerGroupOffsetsAsync(new[] { request }, options)
                    .ConfigureAwait(false);

                var response = results.FirstOrDefault();
                var partitions = response?.Partitions ?? new List<TopicPartitionOffsetError>();
                return partitions.ToList();
            },
            cancellationToken).ConfigureAwait(false);

        if (partitions.Count == 0)
        {
            return Array.Empty<ConsumerGroupLag>();
        }

        var endOffsets = await FetchEndOffsetsAsync(partitions, cancellationToken).ConfigureAwait(false);
        var timestamp = _clock.UtcNow.UtcDateTime;
        var lags = partitions
            .Select(p =>
            {
                var committed = p.Offset < 0 ? 0 : p.Offset.Value;
                var key = new TopicPartition(p.Topic, p.Partition);
                var end = endOffsets.TryGetValue(key, out var value) ? value : committed;
                return new ConsumerGroupLag
                {
                    ConsumerGroupId = consumerGroupId,
                    TopicName = p.Topic,
                    Partition = p.Partition,
                    CommittedOffset = committed,
                    EndOffset = end,
                    EndOffsetTimestampUtc = timestamp
                };
            })
            .Where(l => !query.ExcludeEmptyTopics || l.Lag > 0)
            .ToList();

        return lags;
    }

    private async Task<Dictionary<TopicPartition, long>> FetchEndOffsetsAsync(
        IReadOnlyCollection<TopicPartitionOffsetError> partitions,
        CancellationToken cancellationToken)
    {
        var results = new ConcurrentDictionary<TopicPartition, long>();
        var parallelOptions = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Max(1, _options.MaxDegreeOfParallelism)
        };

        await Parallel.ForEachAsync(partitions, parallelOptions, async (partition, ct) =>
        {
            using var consumer = _consumerFactory.CreateConsumer();
            var tp = new TopicPartition(partition.Topic, partition.Partition);
            var offsets = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromMilliseconds(_options.RequestTimeoutMs));
            results[tp] = offsets.High;
            consumer.Close();
            await Task.CompletedTask;
        }).ConfigureAwait(false);

        return new Dictionary<TopicPartition, long>(results);
    }

    private async Task<List<TopicPartition>> BuildTopicPartitionsAsync(
        IReadOnlyList<string> topics,
        CancellationToken cancellationToken)
    {
        var partitions = new List<TopicPartition>();
        foreach (var topic in topics)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var metadata = await GetTopicMetadataAsync(topic, cancellationToken).ConfigureAwait(false);
            if (!metadata.Exists || metadata.PartitionCount == 0)
            {
                continue;
            }

            for (var partition = 0; partition < metadata.PartitionCount; partition++)
            {
                partitions.Add(new TopicPartition(topic, partition));
            }
        }

        return partitions;
    }

    private async Task<T> ExecuteWithRetryAsync<T>(
        string operation,
        string? topic,
        string? groupId,
        Func<CancellationToken, Task<T>> action,
        CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var attemptScope = _metrics.TrackCall(operation, topic, groupId);
            try
            {
                var result = await action(cancellationToken).ConfigureAwait(false);
                return result;
            }
            catch (KafkaException ex) when (IsRetriable(ex) && attempt < _options.Retry.MaxRetries)
            {
                attemptScope.MarkError(ex);
                attempt++;
                var delay = CalculateDelay(attempt);
                _logger.LogWarning(ex, "Kafka admin operation {Operation} failed (attempt {Attempt}/{MaxAttempts}); retrying in {Delay} ms.", operation, attempt, _options.Retry.MaxRetries, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (KafkaException ex)
            {
                attemptScope.MarkError(ex);
                throw;
            }
            catch (Exception ex)
            {
                attemptScope.MarkError(ex);
                throw;
            }
        }
    }

    private TimeSpan CalculateDelay(int attempt)
    {
        var initial = _options.Retry.InitialBackoffMs;
        var max = _options.Retry.MaxBackoffMs;
        var backoff = Math.Min(initial * Math.Pow(2, attempt - 1), max);
        return TimeSpan.FromMilliseconds(backoff);
    }

    private static bool IsRetriable(KafkaException ex) => !ex.Error.IsFatal;

    private static AdminClientConfig BuildAdminConfig(KafkaAdminOptions options)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = string.IsNullOrWhiteSpace(options.ClientId)
                ? $"khaos-admin-{Environment.MachineName}"
                : options.ClientId,
            SocketTimeoutMs = options.RequestTimeoutMs,
        };

        KafkaConsumerFactory.ApplySecurity(config, options.Security);
        return config;
    }
}
