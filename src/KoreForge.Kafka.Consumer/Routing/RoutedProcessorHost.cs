using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Consumer.Routing;

/// <summary>
/// Implementation of IRoutedProcessorHost that manages bucket workers and routing.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the message.</typeparam>
public sealed class RoutedProcessorHost<TKey, TMessage> : IRoutedProcessorHost
{
    private readonly BucketArray<TKey, TMessage> _bucketArray;
    private readonly BucketWorker<TKey, TMessage>[] _workers;
    private readonly BackpressureCoordinator<TKey, TMessage> _backpressureCoordinator;
    private readonly RoutedProcessorOptions _options;
    private readonly ILogger? _logger;

    private Task[]? _workerTasks;
    private CancellationTokenSource? _cts;

    private long _totalMessagesReceived;
    private long _totalMessagesRouted;
    private long _totalMessagesDiscarded;
    private long _totalMessagesDropped;

    private volatile bool _isRunning;
    private readonly object _lock = new();

    public RoutedProcessorHost(
        IBucketProcessor<TKey, TMessage> processor,
        RoutedProcessorOptions options,
        Func<TKey, int>? hashFunction = null,
        ILogger? logger = null)
        : this(
            bucketIndex => processor,
            options,
            hashFunction,
            logger)
    {
    }

    public RoutedProcessorHost(
        Func<int, IBucketProcessor<TKey, TMessage>> processorFactory,
        RoutedProcessorOptions options,
        Func<TKey, int>? hashFunction = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(processorFactory);
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();
        _options = options;
        _logger = logger;

        _bucketArray = new BucketArray<TKey, TMessage>(
            options.BucketCount,
            options.BucketQueueCapacity,
            hashFunction);

        _backpressureCoordinator = new BackpressureCoordinator<TKey, TMessage>(
            _bucketArray,
            options.BackpressureThresholdPercent,
            options.BackpressureResumePercent);

        _backpressureCoordinator.StateChanged += OnBackpressureStateChanged;

        // Create workers
        _workers = new BucketWorker<TKey, TMessage>[options.BucketCount];
        for (int i = 0; i < options.BucketCount; i++)
        {
            var processor = processorFactory(i);
            _workers[i] = new BucketWorker<TKey, TMessage>(
                i,
                _bucketArray.GetBucketReader(i),
                processor,
                options.MaxBatchSize,
                options.BatchTimeout,
                logger);
        }
    }

    /// <inheritdoc />
    public int BucketCount => _options.BucketCount;

    /// <inheritdoc />
    public int ConsumerThreadCount => _options.ConsumerThreadCount;

    /// <inheritdoc />
    public bool IsBackpressureActive => _backpressureCoordinator.IsActive;

    /// <inheritdoc />
    public event EventHandler<BackpressureEventArgs>? BackpressureChanged;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (_isRunning)
            {
                throw new InvalidOperationException("Host is already running");
            }

            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _workerTasks = new Task[_workers.Length];

            for (int i = 0; i < _workers.Length; i++)
            {
                var worker = _workers[i];
                _workerTasks[i] = Task.Run(() => worker.RunAsync(_cts.Token), _cts.Token);
            }

            _isRunning = true;
            _logger?.LogInformation("RoutedProcessorHost started with {BucketCount} buckets", BucketCount);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task[]? tasks;

        lock (_lock)
        {
            if (!_isRunning)
            {
                return;
            }

            _logger?.LogInformation("RoutedProcessorHost stopping...");

            // Signal bucket channels to complete
            _bucketArray.CompleteAll();

            tasks = _workerTasks;
            _isRunning = false;
        }

        if (tasks != null)
        {
            try
            {
                await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;

        _logger?.LogInformation("RoutedProcessorHost stopped");
    }

    /// <inheritdoc />
    public RoutedProcessorStats GetStats()
    {
        var bucketStats = new BucketStats[_workers.Length];

        for (int i = 0; i < _workers.Length; i++)
        {
            bucketStats[i] = new BucketStats
            {
                BucketIndex = i,
                QueueDepth = _bucketArray.GetQueueDepth(i),
                QueueCapacity = _bucketArray.QueueCapacity,
                FillPercent = _bucketArray.GetFillPercent(i),
                MessagesProcessed = _workers[i].MessagesProcessed,
                BatchesProcessed = _workers[i].BatchesProcessed
            };
        }

        return new RoutedProcessorStats
        {
            TotalMessagesReceived = Interlocked.Read(ref _totalMessagesReceived),
            TotalMessagesRouted = Interlocked.Read(ref _totalMessagesRouted),
            TotalMessagesDiscarded = Interlocked.Read(ref _totalMessagesDiscarded),
            TotalMessagesDropped = Interlocked.Read(ref _totalMessagesDropped),
            Buckets = bucketStats
        };
    }

    /// <summary>
    /// Routes a message to the appropriate bucket based on its routing key.
    /// </summary>
    /// <param name="message">The message to route.</param>
    /// <returns>True if routed successfully, false if dropped due to backpressure.</returns>
    public bool TryRoute(in RoutedMessage<TKey, TMessage> message)
    {
        Interlocked.Increment(ref _totalMessagesReceived);

        // Check backpressure
        _backpressureCoordinator.CheckAndUpdate();

        if (_options.BackpressureBehavior == BackpressureBehavior.DropMessages &&
            _backpressureCoordinator.IsActive)
        {
            Interlocked.Increment(ref _totalMessagesDropped);
            return false;
        }

        if (_bucketArray.TryRoute(in message))
        {
            Interlocked.Increment(ref _totalMessagesRouted);
            return true;
        }

        // Queue full
        if (_options.BackpressureBehavior == BackpressureBehavior.DropMessages)
        {
            Interlocked.Increment(ref _totalMessagesDropped);
            return false;
        }

        // For PauseConsumption or BlockUntilSpace, caller should handle
        return false;
    }

    /// <summary>
    /// Routes a message to the appropriate bucket, waiting if necessary.
    /// </summary>
    /// <param name="message">The message to route.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask RouteAsync(RoutedMessage<TKey, TMessage> message, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _totalMessagesReceived);

        await _bucketArray.RouteAsync(in message, cancellationToken).ConfigureAwait(false);

        Interlocked.Increment(ref _totalMessagesRouted);
    }

    /// <summary>
    /// Records a discarded message (e.g., parse failure).
    /// </summary>
    public void RecordDiscarded()
    {
        Interlocked.Increment(ref _totalMessagesDiscarded);
    }

    /// <summary>
    /// Gets the bucket index for a given routing key.
    /// </summary>
    public int GetBucketIndex(TKey key)
    {
        return _bucketArray.GetBucketIndex(key);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    private void OnBackpressureStateChanged(object? sender, BackpressureEventArgs e)
    {
        _logger?.LogWarning("Backpressure {State} at {FillPercent}% fill",
            e.IsActive ? "activated" : "deactivated",
            e.MaxFillPercent);

        BackpressureChanged?.Invoke(this, e);
    }
}
