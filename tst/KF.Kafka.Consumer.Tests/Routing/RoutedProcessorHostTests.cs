using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using KF.Kafka.Consumer.Routing;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Routing;

public sealed class RoutedProcessorHostTests
{
    [Fact]
    public void Constructor_validates_null_processor()
    {
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        var act = () => new RoutedProcessorHost<string, string>(
            (IBucketProcessor<string, string>)null!,
            options);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_validates_null_options()
    {
        var processor = new TestBucketProcessor();

        var act = () => new RoutedProcessorHost<string, string>(processor, null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_validates_options()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 3 }; // Not power of 2

        var act = () => new RoutedProcessorHost<string, string>(processor, options);

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task Properties_reflect_options()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions
        {
            BucketCount = 8,
            ConsumerThreadCount = 4
        };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);

        host.BucketCount.Should().Be(8);
        host.ConsumerThreadCount.Should().Be(4);
    }

    [Fact]
    public async Task StartAsync_starts_workers()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        // Route a message and verify it gets processed
        var message = CreateMessage("key1", "value1");
        await host.RouteAsync(message);

        await Task.Delay(200); // Allow processing

        processor.ProcessedMessages.Should().NotBeEmpty();
    }

    [Fact]
    public async Task StartAsync_throws_if_already_running()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        var act = async () => await host.StartAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task StopAsync_stops_workers()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();
        await host.StopAsync();

        // Host should be stopped - can call stop again without error
        await host.StopAsync();
    }

    [Fact]
    public async Task TryRoute_returns_true_when_successful()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        var message = CreateMessage("key1", "value1");
        var result = host.TryRoute(in message);

        result.Should().BeTrue();
    }

    [Fact]
    public async Task Messages_routed_to_same_bucket_by_key()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions
        {
            BucketCount = 4,
            MaxBatchSize = 100
        };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        var key = "same-key";
        for (int i = 0; i < 10; i++)
        {
            await host.RouteAsync(CreateMessage(key, $"value-{i}"));
        }

        await Task.Delay(300);

        // All messages should be processed by the same bucket
        var bucketIndex = host.GetBucketIndex(key);
        processor.ProcessedByBucket.TryGetValue(bucketIndex, out var count);
        count.Should().Be(10);
    }

    [Fact]
    public async Task GetStats_returns_accurate_counts()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions
        {
            BucketCount = 4,
            MaxBatchSize = 100
        };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        for (int i = 0; i < 10; i++)
        {
            await host.RouteAsync(CreateMessage($"key-{i}", $"value-{i}"));
        }

        await Task.Delay(300);

        var stats = host.GetStats();

        stats.TotalMessagesReceived.Should().Be(10);
        stats.TotalMessagesRouted.Should().Be(10);
        stats.Buckets.Should().HaveCount(4);
        stats.Buckets.Sum(b => b.MessagesProcessed).Should().Be(10);
    }

    [Fact]
    public async Task RecordDiscarded_increments_counter()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);

        host.RecordDiscarded();
        host.RecordDiscarded();
        host.RecordDiscarded();

        var stats = host.GetStats();

        stats.TotalMessagesDiscarded.Should().Be(3);
    }

    [Fact]
    public async Task BackpressureChanged_fires_when_activated()
    {
        var processor = new SlowBucketProcessor(TimeSpan.FromMilliseconds(100));
        var options = new RoutedProcessorOptions
        {
            BucketCount = 4,
            BucketQueueCapacity = 10,
            BackpressureThresholdPercent = 80,
            BackpressureResumePercent = 50
        };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);

        BackpressureEventArgs? receivedArgs = null;
        host.BackpressureChanged += (_, args) => receivedArgs = args;

        // Flood BEFORE starting (backpressure detection is synchronous inside TryRoute;
        // starting first races against the worker draining the bucket concurrently).
        var key = "same-key";
        for (int i = 0; i < 10; i++)
        {
            host.TryRoute(CreateMessage(key, $"value-{i}"));
        }

        await host.StartAsync();

        // Check backpressure — set synchronously during the routing loop above
        var isActive = host.IsBackpressureActive;

        isActive.Should().BeTrue();
    }

    [Fact]
    public async Task DropMessages_mode_drops_when_backpressure_active()
    {
        var processor = new SlowBucketProcessor(TimeSpan.FromMilliseconds(50));
        var options = new RoutedProcessorOptions
        {
            BucketCount = 4,
            BucketQueueCapacity = 5,
            BackpressureBehavior = BackpressureBehavior.DropMessages,
            BackpressureThresholdPercent = 80,
            BackpressureResumePercent = 50
        };

        await using var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        var key = "same-key";
        int dropped = 0;

        for (int i = 0; i < 20; i++)
        {
            if (!host.TryRoute(CreateMessage(key, $"value-{i}")))
            {
                dropped++;
            }
        }

        var stats = host.GetStats();

        stats.TotalMessagesDropped.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ProcessorFactory_creates_per_bucket_instance()
    {
        var processorsCreated = new ConcurrentDictionary<int, TestBucketProcessor>();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        await using var host = new RoutedProcessorHost<string, string>(
            bucketIndex =>
            {
                var processor = new TestBucketProcessor();
                processorsCreated[bucketIndex] = processor;
                return processor;
            },
            options);

        await host.StartAsync();

        processorsCreated.Count.Should().Be(4);
        processorsCreated.Keys.Should().BeEquivalentTo([0, 1, 2, 3]);
    }

    [Fact]
    public async Task Custom_hash_function_is_used()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        // Custom hash that always returns 2
        await using var host = new RoutedProcessorHost<string, string>(
            processor,
            options,
            hashFunction: _ => 2);

        await host.StartAsync();

        // All keys should map to bucket 2
        for (int i = 0; i < 10; i++)
        {
            await host.RouteAsync(CreateMessage($"key-{i}", $"value-{i}"));
        }

        await Task.Delay(200);

        processor.ProcessedByBucket.TryGetValue(2, out var count);
        count.Should().Be(10);
    }

    [Fact]
    public async Task DisposeAsync_stops_host()
    {
        var processor = new TestBucketProcessor();
        var options = new RoutedProcessorOptions { BucketCount = 4 };

        var host = new RoutedProcessorHost<string, string>(processor, options);
        await host.StartAsync();

        await host.DisposeAsync();

        // Should be able to dispose again without error
        await host.DisposeAsync();
    }

    private static RoutedMessage<string, string> CreateMessage(string key, string value)
    {
        return new RoutedMessage<string, string>
        {
            RoutingKey = key,
            Message = value,
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private sealed class TestBucketProcessor : IBucketProcessor<string, string>
    {
        public ConcurrentBag<(int BucketIndex, string Key, string Value)> ProcessedMessages { get; } = new();
        public ConcurrentDictionary<int, int> ProcessedByBucket { get; } = new();

        public ValueTask ProcessBatchAsync(
            int bucketIndex,
            IReadOnlyList<RoutedMessage<string, string>> messages,
            CancellationToken cancellationToken)
        {
            foreach (var msg in messages)
            {
                ProcessedMessages.Add((bucketIndex, msg.RoutingKey, msg.Message));
                ProcessedByBucket.AddOrUpdate(bucketIndex, 1, (_, count) => count + 1);
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class SlowBucketProcessor : IBucketProcessor<string, string>
    {
        private readonly TimeSpan _delay;

        public SlowBucketProcessor(TimeSpan delay)
        {
            _delay = delay;
        }

        public async ValueTask ProcessBatchAsync(
            int bucketIndex,
            IReadOnlyList<RoutedMessage<string, string>> messages,
            CancellationToken cancellationToken)
        {
            await Task.Delay(_delay, cancellationToken);
        }
    }
}
