using System.Collections.Concurrent;
using KF.Kafka.AdminClient.Instrumentation;
using KF.Time;

namespace KF.Kafka.AdminClient.Caching;

internal sealed class KafkaAdminCache<TKey, TValue>
    where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, CacheEntry> _cache = new();
    private readonly TimeSpan _ttl;
    private readonly bool _enabled;
    private readonly string _name;
    private readonly IKafkaAdminMetrics _metrics;
    private readonly ISystemClock _clock;

    public KafkaAdminCache(string name, bool enabled, TimeSpan ttl, IKafkaAdminMetrics metrics, ISystemClock? clock = null)
    {
        _name = name;
        _enabled = enabled && ttl > TimeSpan.Zero;
        _ttl = ttl;
        _metrics = metrics;
        _clock = clock ?? SystemClock.Instance;
    }

    public bool TryGet(TKey key, out TValue? value)
    {
        value = default;
        if (!_enabled)
        {
            return false;
        }

        if (_cache.TryGetValue(key, out var entry) && !entry.IsExpired(_ttl, _clock.UtcNow))
        {
            value = entry.Value;
            _metrics.RecordCacheHit(_name);
            return true;
        }

        _cache.TryRemove(key, out _);
        _metrics.RecordCacheMiss(_name);

        return false;
    }

    public void Set(TKey key, TValue value)
    {
        if (!_enabled)
        {
            return;
        }

        _cache[key] = new CacheEntry(value, _clock.UtcNow);
    }

    private sealed record CacheEntry(TValue Value, DateTimeOffset TimestampUtc)
    {
        public bool IsExpired(TimeSpan ttl, DateTimeOffset now) => now - TimestampUtc > ttl;
    }
}
