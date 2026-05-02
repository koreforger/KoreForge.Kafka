using System;
using KoreForge.Kafka.Consumer.Abstractions;

namespace KoreForge.Kafka.Consumer.Backpressure;

/// <summary>
/// Simple backpressure policy that pauses once utilization crosses a high watermark
/// and resumes when it falls below a low watermark.
/// </summary>
public sealed class ThresholdBackpressurePolicy : IBackpressurePolicy
{
    private readonly double _pauseThreshold;
    private readonly double _resumeThreshold;

    public ThresholdBackpressurePolicy(double pauseThreshold = 0.9, double resumeThreshold = 0.5)
    {
        if (pauseThreshold is < 0 or > 1) throw new ArgumentOutOfRangeException(nameof(pauseThreshold));
        if (resumeThreshold is < 0 or > 1) throw new ArgumentOutOfRangeException(nameof(resumeThreshold));
        if (resumeThreshold >= pauseThreshold)
        {
            throw new ArgumentException("Resume threshold must be lower than pause threshold.", nameof(resumeThreshold));
        }

        _pauseThreshold = pauseThreshold;
        _resumeThreshold = resumeThreshold;
    }

    public BackpressureDecision Evaluate(BackpressureContext context)
    {
        if (context.Capacity <= 0)
        {
            return BackpressureDecision.None;
        }

        var utilization = context.Utilization;
        if (utilization >= _pauseThreshold)
        {
            return BackpressureDecision.Pause;
        }

        if (utilization <= _resumeThreshold)
        {
            return BackpressureDecision.Resume;
        }

        return BackpressureDecision.None;
    }
}
