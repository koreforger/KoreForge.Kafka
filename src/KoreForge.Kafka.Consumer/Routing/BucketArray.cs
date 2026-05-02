using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KoreForge.Kafka.Consumer.Routing;

/// <summary>
/// Lock-free bucket array for routing messages by key.
/// Uses power-of-2 bucket count for fast modulo via bitmask.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the message.</typeparam>
internal sealed class BucketArray<TKey, TMessage>
{
    private readonly Channel<RoutedMessage<TKey, TMessage>>[] _buckets;
    private readonly int _bucketMask;
    private readonly int _queueCapacity;
    private readonly Func<TKey, int> _hashFunction;

    public BucketArray(int bucketCount, int queueCapacity, Func<TKey, int>? hashFunction = null)
    {
        if (!BitOperations.IsPow2(bucketCount))
            throw new ArgumentException("Bucket count must be a power of 2", nameof(bucketCount));

        if (queueCapacity < 1)
            throw new ArgumentOutOfRangeException(nameof(queueCapacity), "Must be at least 1");

        BucketCount = bucketCount;
        _bucketMask = bucketCount - 1;
        _queueCapacity = queueCapacity;
        _hashFunction = hashFunction ?? DefaultHash;

        // Pre-allocate all bucket channels
        _buckets = new Channel<RoutedMessage<TKey, TMessage>>[bucketCount];
        for (int i = 0; i < bucketCount; i++)
        {
            _buckets[i] = Channel.CreateBounded<RoutedMessage<TKey, TMessage>>(
                new BoundedChannelOptions(queueCapacity)
                {
                    SingleReader = true,   // One worker per bucket
                    SingleWriter = false,  // Multiple consumer threads may route here
                    FullMode = BoundedChannelFullMode.Wait
                });
        }
    }

    /// <summary>
    /// Number of buckets.
    /// </summary>
    public int BucketCount { get; }

    /// <summary>
    /// Queue capacity per bucket.
    /// </summary>
    public int QueueCapacity => _queueCapacity;

    /// <summary>
    /// Computes the bucket index for a given routing key.
    /// Uses bitmask for fast modulo (power-of-2 bucket count).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetBucketIndex(TKey key)
    {
        var hash = _hashFunction(key);
        // Ensure non-negative by masking sign bit, then apply bucket mask
        return (hash & 0x7FFFFFFF) & _bucketMask;
    }

    /// <summary>
    /// Attempts to route a message to its bucket without blocking.
    /// </summary>
    /// <param name="message">The message to route.</param>
    /// <returns>True if successfully queued, false if the bucket queue is full.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRoute(in RoutedMessage<TKey, TMessage> message)
    {
        int index = GetBucketIndex(message.RoutingKey);
        return _buckets[index].Writer.TryWrite(message);
    }

    /// <summary>
    /// Routes a message to its bucket, waiting if the queue is full.
    /// </summary>
    /// <param name="message">The message to route.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public ValueTask RouteAsync(in RoutedMessage<TKey, TMessage> message, CancellationToken cancellationToken)
    {
        int index = GetBucketIndex(message.RoutingKey);
        return _buckets[index].Writer.WriteAsync(message, cancellationToken);
    }

    /// <summary>
    /// Gets the reader for a specific bucket. Used by bucket workers.
    /// </summary>
    public ChannelReader<RoutedMessage<TKey, TMessage>> GetBucketReader(int bucketIndex)
    {
        if (bucketIndex < 0 || bucketIndex >= BucketCount)
            throw new ArgumentOutOfRangeException(nameof(bucketIndex));

        return _buckets[bucketIndex].Reader;
    }

    /// <summary>
    /// Gets the current queue depth for a bucket.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetQueueDepth(int bucketIndex)
    {
        return _buckets[bucketIndex].Reader.Count;
    }

    /// <summary>
    /// Gets the fill percentage for a bucket.
    /// </summary>
    public int GetFillPercent(int bucketIndex)
    {
        var depth = GetQueueDepth(bucketIndex);
        return (int)((depth * 100L) / _queueCapacity);
    }

    /// <summary>
    /// Completes all bucket writers, signaling workers to drain and stop.
    /// </summary>
    public void CompleteAll()
    {
        for (int i = 0; i < BucketCount; i++)
        {
            _buckets[i].Writer.TryComplete();
        }
    }

    private static int DefaultHash(TKey key)
    {
        return key?.GetHashCode() ?? 0;
    }
}
