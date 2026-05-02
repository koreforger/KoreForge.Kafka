using System;
using Microsoft.Extensions.Options;

namespace KoreForge.Kafka.Configuration.Tests.Support;

internal sealed class TestOptionsMonitor<T> : IOptionsMonitor<T>
    where T : class
{
    private T _currentValue;
    private event Action<T, string>? Listeners;

    public TestOptionsMonitor(T currentValue)
    {
        _currentValue = currentValue ?? throw new ArgumentNullException(nameof(currentValue));
    }

    public T CurrentValue => _currentValue;

    public T Get(string? name) => _currentValue;

    public IDisposable OnChange(Action<T, string> listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        Listeners += listener;
        return new Subscription(this, listener);
    }

    public void Update(T nextValue)
    {
        _currentValue = nextValue ?? throw new ArgumentNullException(nameof(nextValue));
        Listeners?.Invoke(_currentValue, Microsoft.Extensions.Options.Options.DefaultName);
    }

    private void Unsubscribe(Action<T, string> listener)
    {
        Listeners -= listener;
    }

    private sealed class Subscription : IDisposable
    {
        private TestOptionsMonitor<T>? _owner;
        private Action<T, string>? _listener;

        public Subscription(TestOptionsMonitor<T> owner, Action<T, string> listener)
        {
            _owner = owner;
            _listener = listener;
        }

        public void Dispose()
        {
            if (_owner is null || _listener is null)
            {
                return;
            }

            _owner.Unsubscribe(_listener);
            _owner = null;
            _listener = null;
        }
    }
}
