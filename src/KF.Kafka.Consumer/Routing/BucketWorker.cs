using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace KF.Kafka.Consumer.Routing;

/// <summary>
/// Worker that processes messages from a single bucket.
/// Runs on its own thread with single-reader semantics.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the message.</typeparam>
internal sealed class BucketWorker<TKey, TMessage>
{
    private readonly int _bucketIndex;
    private readonly ChannelReader<RoutedMessage<TKey, TMessage>> _reader;
    private readonly IBucketProcessor<TKey, TMessage> _processor;
    private readonly int _maxBatchSize;
    private readonly TimeSpan _batchTimeout;
    private readonly ILogger? _logger;

    private long _messagesProcessed;
    private long _batchesProcessed;

    public BucketWorker(
        int bucketIndex,
        ChannelReader<RoutedMessage<TKey, TMessage>> reader,
        IBucketProcessor<TKey, TMessage> processor,
        int maxBatchSize,
        TimeSpan batchTimeout,
        ILogger? logger = null)
    {
        _bucketIndex = bucketIndex;
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _maxBatchSize = maxBatchSize > 0 ? maxBatchSize : throw new ArgumentOutOfRangeException(nameof(maxBatchSize));
        _batchTimeout = batchTimeout > TimeSpan.Zero ? batchTimeout : throw new ArgumentOutOfRangeException(nameof(batchTimeout));
        _logger = logger;
    }

    public int BucketIndex => _bucketIndex;
    public long MessagesProcessed => Interlocked.Read(ref _messagesProcessed);
    public long BatchesProcessed => Interlocked.Read(ref _batchesProcessed);

    /// <summary>
    /// Runs the bucket worker loop until cancelled or the channel completes.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var batch = new List<RoutedMessage<TKey, TMessage>>(_maxBatchSize);

        _logger?.LogDebug("Bucket worker {BucketIndex} started", _bucketIndex);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                batch.Clear();

                try
                {
                    // Wait for the first message
                    if (!await _reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        // Channel completed
                        break;
                    }

                    // Collect messages into batch
                    await CollectBatchAsync(batch, cancellationToken).ConfigureAwait(false);

                    if (batch.Count > 0)
                    {
                        await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Bucket worker {BucketIndex} encountered error processing batch", _bucketIndex);
                }
            }

            // Drain remaining messages on shutdown
            await DrainRemainingAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _logger?.LogDebug("Bucket worker {BucketIndex} stopped. Processed {MessageCount} messages in {BatchCount} batches",
                _bucketIndex, _messagesProcessed, _batchesProcessed);
        }
    }

    private async ValueTask CollectBatchAsync(List<RoutedMessage<TKey, TMessage>> batch, CancellationToken cancellationToken)
    {
        // Start batch timeout
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_batchTimeout);

        try
        {
            while (batch.Count < _maxBatchSize)
            {
                if (_reader.TryRead(out var message))
                {
                    batch.Add(message);
                }
                else
                {
                    // No more messages immediately available - wait briefly
                    if (!await _reader.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                    {
                        // Channel completed
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Batch timeout - process what we have
        }
    }

    private async ValueTask ProcessBatchAsync(List<RoutedMessage<TKey, TMessage>> batch, CancellationToken cancellationToken)
    {
        await _processor.ProcessBatchAsync(_bucketIndex, batch, cancellationToken).ConfigureAwait(false);

        Interlocked.Add(ref _messagesProcessed, batch.Count);
        Interlocked.Increment(ref _batchesProcessed);
    }

    private async ValueTask DrainRemainingAsync(CancellationToken cancellationToken)
    {
        var batch = new List<RoutedMessage<TKey, TMessage>>(_maxBatchSize);

        while (_reader.TryRead(out var message))
        {
            batch.Add(message);

            if (batch.Count >= _maxBatchSize)
            {
                try
                {
                    await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Bucket worker {BucketIndex} error during drain", _bucketIndex);
                }
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            try
            {
                await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Bucket worker {BucketIndex} error during final drain", _bucketIndex);
            }
        }
    }
}
