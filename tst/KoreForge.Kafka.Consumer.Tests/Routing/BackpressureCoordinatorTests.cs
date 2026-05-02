using System;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Routing;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Routing;

public sealed class BackpressureCoordinatorTests
{
    [Fact]
    public void Constructor_validates_null_buckets()
    {
        var act = () => new BackpressureCoordinator<string, string>(null!, 80, 50);

        act.Should().Throw<ArgumentNullException>().And.ParamName.Should().Be("buckets");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(101)]
    public void Constructor_validates_threshold_percent(int percent)
    {
        var buckets = new BucketArray<string, string>(4, 100);

        var act = () => new BackpressureCoordinator<string, string>(buckets, percent, 50);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(80)] // Equal to threshold
    [InlineData(90)] // Greater than threshold
    public void Constructor_validates_resume_percent(int percent)
    {
        var buckets = new BucketArray<string, string>(4, 100);

        var act = () => new BackpressureCoordinator<string, string>(buckets, 80, percent);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void IsActive_initially_false()
    {
        var buckets = new BucketArray<string, string>(4, 100);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);

        coordinator.IsActive.Should().BeFalse();
    }

    [Fact]
    public void CheckAndUpdate_activates_when_threshold_exceeded()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);
        var key = "key1";
        var bucketIndex = buckets.GetBucketIndex(key);

        // Fill bucket to 80%
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }

        var maxFill = coordinator.CheckAndUpdate();

        coordinator.IsActive.Should().BeTrue();
        maxFill.Should().BeGreaterOrEqualTo(80);
    }

    [Fact]
    public void CheckAndUpdate_deactivates_when_below_resume()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);
        var key = "key1";

        // Fill bucket to 80% and activate
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }
        coordinator.CheckAndUpdate();
        coordinator.IsActive.Should().BeTrue();

        // Drain by reading from the channel
        var reader = buckets.GetBucketReader(buckets.GetBucketIndex(key));
        for (int i = 0; i < 6; i++) // Drain to below 50%
        {
            reader.TryRead(out _);
        }

        coordinator.CheckAndUpdate();

        coordinator.IsActive.Should().BeFalse();
    }

    [Fact]
    public void CheckAndUpdate_remains_active_between_thresholds()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 30);
        var key = "key1";

        // Fill bucket to 80% and activate
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }
        coordinator.CheckAndUpdate();
        coordinator.IsActive.Should().BeTrue();

        // Drain to 50% (between thresholds)
        var reader = buckets.GetBucketReader(buckets.GetBucketIndex(key));
        for (int i = 0; i < 3; i++)
        {
            reader.TryRead(out _);
        }

        coordinator.CheckAndUpdate();

        coordinator.IsActive.Should().BeTrue(); // Still active
    }

    [Fact]
    public void StateChanged_fires_on_activation()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);
        var key = "key1";

        BackpressureEventArgs? receivedArgs = null;
        coordinator.StateChanged += (_, args) => receivedArgs = args;

        // Fill bucket to 80%
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }

        coordinator.CheckAndUpdate();

        receivedArgs.Should().NotBeNull();
        receivedArgs!.IsActive.Should().BeTrue();
        receivedArgs.MaxFillPercent.Should().BeGreaterOrEqualTo(80);
    }

    [Fact]
    public void StateChanged_fires_on_deactivation()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);
        var key = "key1";

        // Activate first
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }
        coordinator.CheckAndUpdate();

        BackpressureEventArgs? receivedArgs = null;
        coordinator.StateChanged += (_, args) => receivedArgs = args;

        // Drain below 50%
        var reader = buckets.GetBucketReader(buckets.GetBucketIndex(key));
        for (int i = 0; i < 6; i++)
        {
            reader.TryRead(out _);
        }

        coordinator.CheckAndUpdate();

        receivedArgs.Should().NotBeNull();
        receivedArgs!.IsActive.Should().BeFalse();
    }

    [Fact]
    public void StateChanged_does_not_fire_when_no_change()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);

        int eventCount = 0;
        coordinator.StateChanged += (_, _) => eventCount++;

        // Multiple checks with no messages
        coordinator.CheckAndUpdate();
        coordinator.CheckAndUpdate();
        coordinator.CheckAndUpdate();

        eventCount.Should().Be(0);
    }

    [Fact]
    public void Reset_clears_active_state()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);
        var key = "key1";

        // Activate
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(key, $"v{i}"));
        }
        coordinator.CheckAndUpdate();
        coordinator.IsActive.Should().BeTrue();

        coordinator.Reset();

        coordinator.IsActive.Should().BeFalse();
    }

    [Fact]
    public void CheckAndUpdate_considers_all_buckets()
    {
        var buckets = new BucketArray<string, string>(4, 10);
        var coordinator = new BackpressureCoordinator<string, string>(buckets, 80, 50);

        // Find keys that map to different buckets
        var keysByBucket = new string[4];
        for (int i = 0; i < 1000 && keysByBucket.Any(k => k == null); i++)
        {
            var key = $"key-{i}";
            var index = buckets.GetBucketIndex(key);
            if (keysByBucket[index] == null)
            {
                keysByBucket[index] = key;
            }
        }

        // Fill bucket 2 to 80%
        for (int i = 0; i < 8; i++)
        {
            buckets.TryRoute(CreateMessage(keysByBucket[2], $"v{i}"));
        }

        var maxFill = coordinator.CheckAndUpdate();

        coordinator.IsActive.Should().BeTrue();
        maxFill.Should().Be(80);
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
