using System;
using KoreForge.Kafka.Configuration.Options;
using Microsoft.Extensions.Options;

namespace KoreForge.Kafka.Configuration.Profiles;

public sealed class KafkaConfigurationChangedEventArgs : EventArgs
{
    public KafkaConfigurationRootOptions NewOptions { get; }

    public KafkaConfigurationChangedEventArgs(KafkaConfigurationRootOptions newOptions)
    {
        NewOptions = newOptions;
    }
}

public interface IKafkaConfigurationChangeNotifier
{
    event EventHandler<KafkaConfigurationChangedEventArgs>? Changed;
}

public sealed class KafkaConfigurationChangeNotifier : IKafkaConfigurationChangeNotifier, IDisposable
{
    private readonly IDisposable? _subscription;

    public KafkaConfigurationChangeNotifier(IOptionsMonitor<KafkaConfigurationRootOptions> optionsMonitor)
    {
        _subscription = optionsMonitor.OnChange(options =>
        {
            if (options is null)
            {
                return;
            }

            Changed?.Invoke(this, new KafkaConfigurationChangedEventArgs(options));
        });
    }

    public event EventHandler<KafkaConfigurationChangedEventArgs>? Changed;

    public void Dispose()
    {
        _subscription?.Dispose();
    }
}
