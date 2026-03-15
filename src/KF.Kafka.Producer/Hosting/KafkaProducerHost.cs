using System;
using System.Threading;
using System.Threading.Tasks;
using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Core.Alerts;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Alerts;
using KF.Kafka.Producer.Backlog;
using KF.Kafka.Producer.Backpressure;
using KF.Kafka.Producer.Logging;
using KF.Kafka.Producer.Metrics;
using KF.Kafka.Producer.Restart;
using KF.Kafka.Producer.Runtime;
using KF.Kafka.Producer.Serialization;
using KF.Metrics;
using KF.Time;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KF.Kafka.Producer.Hosting;

public sealed class KafkaProducerHostBuilder : IProducerAlertRegistrar
{
    private string? _profileName;
    private IKafkaClientConfigFactory? _configFactory;
    private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
    private readonly ProducerSerializerRegistry _serializerRegistry = new();
    private IProducerBackpressurePolicy? _backpressurePolicy;
    private IProducerRestartPolicy? _restartPolicy;
    private IProducerBacklogPersistenceStrategy? _backlogStrategy;
    private IProducerBacklogPersistenceStrategyFactory? _backlogStrategyFactory;
    private readonly ProducerAlertRegistry _alerts = new();
    private ISystemClock _clock = SystemClock.Instance;
    private IOperationMonitor? _operationMonitor;

    public KafkaProducerHostBuilder UseKafkaConfigurationProfile(string profileName, IKafkaClientConfigFactory factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _profileName = profileName;
        _configFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaProducerHostBuilder UseLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        return this;
    }

    public KafkaProducerHostBuilder UseSerializer<TMessage>(IProducerSerializer<TMessage> serializer)
    {
        _serializerRegistry.RegisterSerializer(serializer);
        return this;
    }

    public KafkaProducerHostBuilder UseJsonSerializer<TMessage>()
    {
        _serializerRegistry.RegisterSerializer(new JsonProducerSerializer<TMessage>());
        return this;
    }

    public KafkaProducerHostBuilder UseSerializerRegistrations(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);

        foreach (var registration in serviceProvider.GetServices<IProducerSerializerRegistration>())
        {
            registration.Register(_serializerRegistry, serviceProvider);
        }

        return this;
    }

    public KafkaProducerHostBuilder UseBackpressurePolicy(IProducerBackpressurePolicy policy)
    {
        _backpressurePolicy = policy ?? throw new ArgumentNullException(nameof(policy));
        return this;
    }

    public KafkaProducerHostBuilder UseRestartPolicy(IProducerRestartPolicy policy)
    {
        _restartPolicy = policy ?? throw new ArgumentNullException(nameof(policy));
        return this;
    }

    public KafkaProducerHostBuilder UseBacklogPersistence(IProducerBacklogPersistenceStrategy strategy)
    {
        _backlogStrategy = strategy ?? throw new ArgumentNullException(nameof(strategy));
        return this;
    }

    public KafkaProducerHostBuilder UseBacklogPersistenceFactory(IProducerBacklogPersistenceStrategyFactory factory)
    {
        _backlogStrategyFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaProducerHostBuilder UseClock(ISystemClock clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }

    public KafkaProducerHostBuilder UseOperationMonitor(IOperationMonitor monitor)
    {
        _operationMonitor = monitor ?? throw new ArgumentNullException(nameof(monitor));
        return this;
    }

    public KafkaProducerHostBuilder RegisterAlert(Func<ProducerStatusSnapshot, bool> condition, Action<ProducerStatusSnapshot> action, AlertOptions? options = null)
    {
        _alerts.RegisterAlert(condition, action, options);
        return this;
    }

    IProducerAlertRegistrar IProducerAlertRegistrar.RegisterAlert(Func<ProducerStatusSnapshot, bool> condition, Action<ProducerStatusSnapshot> action, AlertOptions? options)
        => RegisterAlert(condition, action, options);

    public KafkaProducerHost Build()
    {
        if (_profileName is null)
        {
            throw new InvalidOperationException("Kafka profile name was not configured.");
        }

        if (_configFactory is null)
        {
            throw new InvalidOperationException("Kafka configuration factory is required.");
        }

        var runtimeConfig = _configFactory.CreateProducer(_profileName);
        var backpressure = _backpressurePolicy ?? new DefaultProducerBackpressurePolicy(_loggerFactory.CreateLogger<DefaultProducerBackpressurePolicy>());
        var restart = _restartPolicy ?? new DefaultProducerRestartPolicy(runtimeConfig.Extended.Restart);
        var backlog = _backlogStrategy
            ?? (_backlogStrategyFactory ?? new DefaultProducerBacklogPersistenceStrategyFactory())
                .Create(runtimeConfig.Extended, _loggerFactory);
        var metrics = new ProducerMetricsBridge(_operationMonitor);
        _alerts.AttachObservers(_loggerFactory.CreateLogger<ProducerAlertRegistry>(), metrics);

        var supervisor = new KafkaProducerSupervisor(runtimeConfig, _serializerRegistry, backpressure, restart, backlog, metrics, _clock, _alerts, _loggerFactory);
        return new KafkaProducerHost(supervisor, _loggerFactory);
    }
}

public sealed class KafkaProducerHost : IAsyncDisposable
{
    private readonly KafkaProducerSupervisor _supervisor;
    private readonly ILogger<KafkaProducerHost> _logger;
    private bool _started;

    internal KafkaProducerHost(KafkaProducerSupervisor supervisor, ILoggerFactory loggerFactory)
    {
        _supervisor = supervisor ?? throw new ArgumentNullException(nameof(supervisor));
        _logger = loggerFactory.CreateLogger<KafkaProducerHost>();
    }

    public IProducerBuffer Buffer => _supervisor.Buffer;
    public ProducerRuntimeState RuntimeState => _supervisor.RuntimeState;

    public static KafkaProducerHostBuilder Create() => new();

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_started)
        {
            throw new InvalidOperationException("Kafka producer host already started.");
        }

        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Starting), "Kafka producer host starting");
        await _supervisor.StartAsync(cancellationToken).ConfigureAwait(false);
        _started = true;
        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Started), "Kafka producer host started");
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_started)
        {
            return;
        }

        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Stopping), "Kafka producer host stopping");
        await _supervisor.StopAsync(cancellationToken).ConfigureAwait(false);
        _started = false;
        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Stopped), "Kafka producer host stopped");
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        await _supervisor.DisposeAsync().ConfigureAwait(false);
    }

    private static EventId ToEventId(ProducerLogEvents evt) => new((int)evt, evt.ToString());
}
