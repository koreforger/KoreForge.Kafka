using FluentAssertions;
using KoreForge.Kafka.AdminClient.Caching;
using KoreForge.Kafka.AdminClient.Instrumentation;
using KoreForge.Time;
using Xunit;

namespace KoreForge.Kafka.AdminClient.Tests;

public sealed class KafkaAdminCacheTests
{
    [Fact]
    public void Returns_false_when_disabled()
    {
        var cache = new KafkaAdminCache<string, int>("test", enabled: false, TimeSpan.FromSeconds(1), new TestMetrics(), new VirtualSystemClock());

        cache.TryGet("key", out _).Should().BeFalse();
        cache.Set("key", 42);
        cache.TryGet("key", out _).Should().BeFalse();
    }

    [Fact]
    public void Stores_and_expires_entries()
    {
        var metrics = new TestMetrics();
        var clock = new VirtualSystemClock(DateTimeOffset.Parse("2025-01-01T00:00:00Z"));
        var cache = new KafkaAdminCache<string, int>("test", enabled: true, TimeSpan.FromMilliseconds(5), metrics, clock);

        cache.Set("key", 42);
        cache.TryGet("key", out var value).Should().BeTrue();
        value.Should().Be(42);
        metrics.CacheHits.Should().Be(1);

        clock.Advance(TimeSpan.FromMilliseconds(10));
        cache.TryGet("key", out _).Should().BeFalse();
        metrics.CacheMisses.Should().Be(1);
    }

    [Fact]
    public void Does_not_emit_metrics_when_disabled()
    {
        var metrics = new TestMetrics();
        var cache = new KafkaAdminCache<string, int>("test", enabled: false, TimeSpan.FromSeconds(1), metrics, new VirtualSystemClock());

        cache.TryGet("key", out _);
        cache.Set("key", 5);
        cache.TryGet("key", out _);

        metrics.CacheHits.Should().Be(0);
        metrics.CacheMisses.Should().Be(0);
    }

    [Fact]
    public void Updating_existing_entry_replaces_value()
    {
        var cache = new KafkaAdminCache<string, int>("test", enabled: true, TimeSpan.FromSeconds(1), new TestMetrics(), new VirtualSystemClock());

        cache.Set("key", 1);
        cache.Set("key", 2);

        cache.TryGet("key", out var value).Should().BeTrue();
        value.Should().Be(2);
    }

    private sealed class TestMetrics : IKafkaAdminMetrics
    {
        public int CacheHits { get; private set; }
        public int CacheMisses { get; private set; }

        public void RecordCacheHit(string cacheName) => CacheHits++;
        public void RecordCacheMiss(string cacheName) => CacheMisses++;
        public IKafkaAdminCallScope TrackCall(string operation, string? topic, string? groupId) => NoOpScope.Instance;

        private sealed class NoOpScope : IKafkaAdminCallScope
        {
            public static readonly NoOpScope Instance = new();

            public void MarkError(Exception exception)
            {
            }

            public void Dispose()
            {
            }
        }
    }
}
