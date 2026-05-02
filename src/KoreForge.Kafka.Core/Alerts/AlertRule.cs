using KoreForge.Time;

namespace KoreForge.Kafka.Core.Alerts;

/// <summary>
/// Encapsulates a condition and action pair together with execution state.
/// </summary>
public sealed class AlertRule<TSnapshot>
{
    private readonly Func<TSnapshot, bool> _condition;
    private readonly Action<TSnapshot> _action;
    private readonly AlertOptions _options;
    private readonly ISystemClock _clock;

    private bool _lastEvaluation;
    private DateTimeOffset? _lastFiredAt;

    public AlertRule(
        Func<TSnapshot, bool> condition,
        Action<TSnapshot> action,
        AlertOptions? options = null,
        ISystemClock? clock = null)
    {
        _condition = condition ?? throw new ArgumentNullException(nameof(condition));
        _action = action ?? throw new ArgumentNullException(nameof(action));
        _options = options ?? new AlertOptions();
        _clock = clock ?? SystemClock.Instance;
    }

    public string DisplayName => _options.Name ?? typeof(TSnapshot).Name;

    public void Evaluate(TSnapshot snapshot)
    {
        if (snapshot == null) throw new ArgumentNullException(nameof(snapshot));

        var now = _clock.Now;
        var isTrue = _condition(snapshot);

        if (_options.FireOnTransitionOnly && _lastEvaluation && isTrue)
        {
            return;
        }

        if (_options.MinIntervalBetweenFirings is { } interval && _lastFiredAt is { } lastFire)
        {
            if (now - lastFire < interval)
            {
                _lastEvaluation = isTrue;
                return;
            }
        }

        if (isTrue)
        {
            _lastFiredAt = now;
            _action(snapshot);
        }

        _lastEvaluation = isTrue;
    }
}
