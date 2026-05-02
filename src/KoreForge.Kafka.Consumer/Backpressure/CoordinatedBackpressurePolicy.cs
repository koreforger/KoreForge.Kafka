using System;
using KoreForge.Kafka.Consumer.Abstractions;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Consumer.Backpressure;

/// <summary>
/// Ensures that once any consumer worker triggers backpressure, all workers pause together
/// and only resume when utilization falls below the configured resume threshold.
/// </summary>
public sealed class CoordinatedBackpressurePolicy : IBackpressurePolicy
{
    private readonly IBackpressurePolicy _inner;
    private readonly ILogger? _logger;
    private readonly object _sync = new();
    private bool _clusterPaused;

    public CoordinatedBackpressurePolicy(IBackpressurePolicy inner, ILogger<CoordinatedBackpressurePolicy>? logger = null)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _logger = logger;
    }

    public static CoordinatedBackpressurePolicy CreateThresholdPolicy(
        double pauseThreshold = 1.0,
        double resumeThreshold = 0.5,
        ILogger<CoordinatedBackpressurePolicy>? logger = null) =>
        new(new ThresholdBackpressurePolicy(pauseThreshold, resumeThreshold), logger);

    public BackpressureDecision Evaluate(BackpressureContext context)
    {
        if (context.Capacity <= 0)
        {
            return BackpressureDecision.None;
        }

        var decision = _inner.Evaluate(context);

        lock (_sync)
        {
            if (decision == BackpressureDecision.Pause)
            {
                if (!_clusterPaused)
                {
                    _clusterPaused = true;
                    _logger?.LogInformation(
                        "Coordinated backpressure paused all workers because worker {WorkerId} backlog is {Backlog}/{Capacity} ({Utilization:P0})",
                        context.WorkerId,
                        context.BacklogSize,
                        context.Capacity,
                        context.Utilization);
                }

                return BackpressureDecision.Pause;
            }

            if (decision == BackpressureDecision.Resume)
            {
                if (_clusterPaused)
                {
                    _clusterPaused = false;
                    _logger?.LogInformation(
                        "Coordinated backpressure resumed all workers because worker {WorkerId} backlog dropped to {Backlog}/{Capacity} ({Utilization:P0})",
                        context.WorkerId,
                        context.BacklogSize,
                        context.Capacity,
                        context.Utilization);
                }

                return BackpressureDecision.Resume;
            }

            return _clusterPaused ? BackpressureDecision.Pause : BackpressureDecision.None;
        }
    }
}
