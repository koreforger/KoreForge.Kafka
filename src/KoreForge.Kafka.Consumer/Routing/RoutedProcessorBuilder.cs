using System;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Consumer.Routing;

/// <summary>
/// Fluent builder for creating RoutedProcessorHost instances.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the message.</typeparam>
public sealed class RoutedProcessorBuilder<TKey, TMessage>
{
    private readonly RoutedProcessorOptions _options = new();
    private Func<int, IBucketProcessor<TKey, TMessage>>? _processorFactory;
    private Func<TKey, int>? _hashFunction;
    private ILogger? _logger;

    /// <summary>
    /// Sets the bucket processor (single instance shared across buckets).
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> UseBucketProcessor(
        IBucketProcessor<TKey, TMessage> processor)
    {
        ArgumentNullException.ThrowIfNull(processor);
        _processorFactory = _ => processor;
        return this;
    }

    /// <summary>
    /// Sets the bucket processor using a factory (creates one instance per bucket).
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> UseBucketProcessor(
        Func<int, IBucketProcessor<TKey, TMessage>> factory)
    {
        _processorFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    /// <summary>
    /// Configures the number of consumer threads.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithConsumerThreads(int count)
    {
        _options.ConsumerThreadCount = count;
        return this;
    }

    /// <summary>
    /// Configures the number of processing buckets (must be power of 2).
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithBucketCount(int count)
    {
        _options.BucketCount = count;
        return this;
    }

    /// <summary>
    /// Configures the queue capacity per bucket.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithBucketQueueCapacity(int capacity)
    {
        _options.BucketQueueCapacity = capacity;
        return this;
    }

    /// <summary>
    /// Configures the maximum batch size delivered to processors.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithMaxBatchSize(int size)
    {
        _options.MaxBatchSize = size;
        return this;
    }

    /// <summary>
    /// Configures the batch timeout.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithBatchTimeout(TimeSpan timeout)
    {
        _options.BatchTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Configures backpressure behavior and thresholds.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithBackpressure(
        BackpressureBehavior behavior,
        int thresholdPercent = 80,
        int resumePercent = 50)
    {
        _options.BackpressureBehavior = behavior;
        _options.BackpressureThresholdPercent = thresholdPercent;
        _options.BackpressureResumePercent = resumePercent;
        return this;
    }

    /// <summary>
    /// Sets a custom hash function for routing keys.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithHashFunction(Func<TKey, int> hashFunction)
    {
        _hashFunction = hashFunction ?? throw new ArgumentNullException(nameof(hashFunction));
        return this;
    }

    /// <summary>
    /// Sets the logger.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithLogger(ILogger logger)
    {
        _logger = logger;
        return this;
    }

    /// <summary>
    /// Sets the logger factory.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory?.CreateLogger<RoutedProcessorHost<TKey, TMessage>>();
        return this;
    }

    /// <summary>
    /// Applies configuration from an options instance.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> WithOptions(RoutedProcessorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _options.ConsumerThreadCount = options.ConsumerThreadCount;
        _options.BucketCount = options.BucketCount;
        _options.BucketQueueCapacity = options.BucketQueueCapacity;
        _options.MaxBatchSize = options.MaxBatchSize;
        _options.BatchTimeout = options.BatchTimeout;
        _options.BackpressureBehavior = options.BackpressureBehavior;
        _options.BackpressureThresholdPercent = options.BackpressureThresholdPercent;
        _options.BackpressureResumePercent = options.BackpressureResumePercent;

        return this;
    }

    /// <summary>
    /// Applies configuration using an action.
    /// </summary>
    public RoutedProcessorBuilder<TKey, TMessage> Configure(Action<RoutedProcessorOptions> configure)
    {
        configure?.Invoke(_options);
        return this;
    }

    /// <summary>
    /// Builds the routed processor host.
    /// </summary>
    public RoutedProcessorHost<TKey, TMessage> Build()
    {
        if (_processorFactory == null)
        {
            throw new InvalidOperationException("A bucket processor must be configured. Call UseBucketProcessor().");
        }

        return new RoutedProcessorHost<TKey, TMessage>(
            _processorFactory,
            _options,
            _hashFunction,
            _logger);
    }
}

/// <summary>
/// Factory for creating routed processor builders.
/// </summary>
public static class RoutedProcessor
{
    /// <summary>
    /// Creates a new builder for a routed processor host.
    /// </summary>
    /// <typeparam name="TKey">Type of the routing key.</typeparam>
    /// <typeparam name="TMessage">Type of the message.</typeparam>
    public static RoutedProcessorBuilder<TKey, TMessage> Create<TKey, TMessage>()
        => new();
}
