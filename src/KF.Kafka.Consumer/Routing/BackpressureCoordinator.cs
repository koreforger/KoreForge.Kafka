using System;
using System.Threading;

namespace KF.Kafka.Consumer.Routing;

/// <summary>
/// Monitors bucket queue fill levels and manages backpressure state.
/// </summary>
/// <typeparam name="TKey">Type of the routing key.</typeparam>
/// <typeparam name="TMessage">Type of the message.</typeparam>
internal sealed class BackpressureCoordinator<TKey, TMessage>
{
    private readonly BucketArray<TKey, TMessage> _buckets;
    private readonly int _thresholdPercent;
    private readonly int _resumePercent;
    private volatile bool _isActive;

    public BackpressureCoordinator(
        BucketArray<TKey, TMessage> buckets,
        int thresholdPercent,
        int resumePercent)
    {
        _buckets = buckets ?? throw new ArgumentNullException(nameof(buckets));

        if (thresholdPercent is < 1 or > 100)
            throw new ArgumentOutOfRangeException(nameof(thresholdPercent));

        if (resumePercent < 0 || resumePercent >= thresholdPercent)
            throw new ArgumentOutOfRangeException(nameof(resumePercent));

        _thresholdPercent = thresholdPercent;
        _resumePercent = resumePercent;
    }

    /// <summary>
    /// Returns true if backpressure is currently active.
    /// </summary>
    public bool IsActive => _isActive;

    /// <summary>
    /// Event raised when backpressure state changes.
    /// </summary>
    public event EventHandler<BackpressureEventArgs>? StateChanged;

    /// <summary>
    /// Checks current fill levels and updates backpressure state.
    /// </summary>
    /// <returns>The current maximum fill percentage across all buckets.</returns>
    public int CheckAndUpdate()
    {
        int maxFill = 0;

        for (int i = 0; i < _buckets.BucketCount; i++)
        {
            var fill = _buckets.GetFillPercent(i);
            if (fill > maxFill)
            {
                maxFill = fill;
            }
        }

        bool wasActive = _isActive;

        if (!_isActive && maxFill >= _thresholdPercent)
        {
            _isActive = true;
            StateChanged?.Invoke(this, new BackpressureEventArgs(true, maxFill));
        }
        else if (_isActive && maxFill <= _resumePercent)
        {
            _isActive = false;
            StateChanged?.Invoke(this, new BackpressureEventArgs(false, maxFill));
        }

        return maxFill;
    }

    /// <summary>
    /// Resets backpressure state without triggering events.
    /// </summary>
    public void Reset()
    {
        _isActive = false;
    }
}
