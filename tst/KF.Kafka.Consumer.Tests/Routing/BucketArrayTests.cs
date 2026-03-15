using FluentAssertions;
using KF.Kafka.Consumer.Routing;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Routing;

public sealed class BucketArrayTests
{
    [Fact]
    public void Constructor_validates_power_of_two_bucket_count()
    {
        var act = () => new BucketArray<string, string>(3, 100);
        act.Should().Throw<ArgumentException>().And.ParamName.Should().Be("bucketCount");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    [InlineData(128)]
    public void Constructor_accepts_power_of_two_bucket_count(int bucketCount)
    {
        var buckets = new BucketArray<string, string>(bucketCount, 100);
        buckets.BucketCount.Should().Be(bucketCount);
    }

    [Fact]
    public void Constructor_validates_queue_capacity()
    {
        var act = () => new BucketArray<string, string>(4, 0);
        act.Should().Throw<ArgumentOutOfRangeException>().And.ParamName.Should().Be("queueCapacity");
    }

    [Theory]
    [InlineData("key1")]
    [InlineData("key2")]
    [InlineData("different-key")]
    public void GetBucketIndex_returns_consistent_index_for_same_key(string key)
    {
        var buckets = new BucketArray<string, string>(8, 100);

        var index1 = buckets.GetBucketIndex(key);
        var index2 = buckets.GetBucketIndex(key);
        var index3 = buckets.GetBucketIndex(key);

        index1.Should().Be(index2).And.Be(index3);
    }

    [Fact]
    public void GetBucketIndex_returns_index_within_bounds()
    {
        var buckets = new BucketArray<string, string>(8, 100);

        for (int i = 0; i < 1000; i++)
        {
            var key = $"key-{i}";
            var index = buckets.GetBucketIndex(key);
            index.Should().BeInRange(0, 7);
        }
    }

    [Fact]
    public void GetBucketIndex_distributes_keys_across_buckets()
    {
        var buckets = new BucketArray<string, string>(8, 100);
        var distribution = new int[8];

        // Generate many keys and check distribution
        for (int i = 0; i < 10000; i++)
        {
            var index = buckets.GetBucketIndex($"key-{i}");
            distribution[index]++;
        }

        // Each bucket should have at least some keys (not a perfect distribution test,
        // but ensures basic spread)
        foreach (var count in distribution)
        {
            count.Should().BeGreaterThan(100, "keys should be distributed across buckets");
        }
    }

    [Fact]
    public void GetBucketIndex_handles_null_key()
    {
        var buckets = new BucketArray<string?, string>(4, 100);

        // Should not throw
        var index = buckets.GetBucketIndex(null);
        index.Should().BeInRange(0, 3);
    }

    [Fact]
    public void TryRoute_succeeds_when_queue_not_full()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var message = CreateMessage("key1", "value1");

        var result = buckets.TryRoute(in message);

        result.Should().BeTrue();
        buckets.GetQueueDepth(buckets.GetBucketIndex("key1")).Should().Be(1);
    }

    [Fact]
    public void TryRoute_fails_when_queue_full()
    {
        var buckets = new BucketArray<string, string>(4, 2);
        var key = "key1";
        var bucketIndex = buckets.GetBucketIndex(key);

        // Fill the queue
        buckets.TryRoute(CreateMessage(key, "v1"));
        buckets.TryRoute(CreateMessage(key, "v2"));

        // Third should fail
        var result = buckets.TryRoute(CreateMessage(key, "v3"));

        result.Should().BeFalse();
        buckets.GetQueueDepth(bucketIndex).Should().Be(2);
    }

    [Fact]
    public async Task RouteAsync_succeeds()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var message = CreateMessage("key1", "value1");

        await buckets.RouteAsync(in message, CancellationToken.None);

        buckets.GetQueueDepth(buckets.GetBucketIndex("key1")).Should().Be(1);
    }

    [Fact]
    public void GetQueueDepth_returns_correct_count()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var key = "key1";
        var bucketIndex = buckets.GetBucketIndex(key);

        buckets.GetQueueDepth(bucketIndex).Should().Be(0);

        buckets.TryRoute(CreateMessage(key, "v1"));
        buckets.GetQueueDepth(bucketIndex).Should().Be(1);

        buckets.TryRoute(CreateMessage(key, "v2"));
        buckets.GetQueueDepth(bucketIndex).Should().Be(2);
    }

    [Fact]
    public void GetFillPercent_returns_correct_percentage()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var key = "key1";
        var bucketIndex = buckets.GetBucketIndex(key);

        buckets.GetFillPercent(bucketIndex).Should().Be(0);

        for (int i = 0; i < 5; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }

        buckets.GetFillPercent(bucketIndex).Should().Be(50);
    }

    [Fact]
    public void GetBucketReader_returns_reader()
    {
        var buckets = new BucketArray<string, string>(4, 10);

        var reader = buckets.GetBucketReader(0);

        reader.Should().NotBeNull();
    }

    [Fact]
    public void GetBucketReader_throws_for_invalid_index()
    {
        var buckets = new BucketArray<string, string>(4, 10);

        var act = () => buckets.GetBucketReader(-1);
        act.Should().Throw<ArgumentOutOfRangeException>();

        act = () => buckets.GetBucketReader(4);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task CompleteAll_signals_readers()
    {
        var buckets = new BucketArray<string, string>(4, 10);

        buckets.CompleteAll();

        // Readers should complete
        for (int i = 0; i < 4; i++)
        {
            var reader = buckets.GetBucketReader(i);
            var completion = await reader.WaitToReadAsync();
            completion.Should().BeFalse();
        }
    }

    [Fact]
    public void Custom_hash_function_is_used()
    {
        // Custom hash that always returns 0
        var buckets = new BucketArray<string, string>(4, 10, _ => 0);

        var index = buckets.GetBucketIndex("any-key");

        index.Should().Be(0);
    }

    [Fact]
    public void Custom_hash_function_negative_values_handled()
    {
        // Custom hash that returns negative
        var buckets = new BucketArray<string, string>(4, 10, _ => -12345);

        var index = buckets.GetBucketIndex("any-key");

        index.Should().BeInRange(0, 3);
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
}
