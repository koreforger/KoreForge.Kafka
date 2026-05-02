using System;
using System.Threading;
using System.Threading.Tasks;

namespace KoreForge.Kafka.Consumer.Routing;

/// <summary>
/// Manages the lifecycle of routed processors: consumers, routers, and bucket workers.
/// </summary>
public interface IRoutedProcessorHost : IAsyncDisposable
{
    /// <summary>
    /// Total number of processing buckets.
    /// </summary>
    int BucketCount { get; }

    /// <summary>
    /// Number of active consumer threads.
    /// </summary>
    int ConsumerThreadCount { get; }

    /// <summary>
    /// Returns true if backpressure is currently active.
    /// </summary>
    bool IsBackpressureActive { get; }

    /// <summary>
    /// Starts all consumer threads and bucket workers.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops all threads gracefully, draining queues.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets current runtime statistics.
    /// </summary>
    RoutedProcessorStats GetStats();

    /// <summary>
    /// Event raised when backpressure state changes.
    /// </summary>
    event EventHandler<BackpressureEventArgs>? BackpressureChanged;
}
