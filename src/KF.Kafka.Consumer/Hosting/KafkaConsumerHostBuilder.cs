using System;
using System.Threading;
using System.Threading.Tasks;
using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Consumer.Abstractions;
using KF.Kafka.Consumer.Backpressure;
using KF.Kafka.Consumer.Execution;
using KF.Kafka.Consumer.Logging;
using KF.Kafka.Consumer.Pipelines;
using KF.Kafka.Consumer.Restart;
using KF.Kafka.Consumer.Runtime;
using KF.Time;
using KoreForge.Processing.Pipelines;
using KoreForge.Processing.Pipelines.Instrumentation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using IOperationMonitor = KF.Metrics.IOperationMonitor;

namespace KF.Kafka.Consumer.Hosting;

public sealed class KafkaConsumerHostBuilder
{
    private string? _profileName;
    private IKafkaClientConfigFactory? _configFactory;
    private Func<IKafkaBatchProcessor>? _processorFactory;
    private Action<ExtendedConsumerSettings>? _consumerSettingsApplicator;
    private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
    private ISystemClock _clock = SystemClock.Instance;
    private IBackpressurePolicy? _backpressurePolicy;
    private IRestartPolicy _restartPolicy = new FixedRetryRestartPolicy();
    private Func<IPipelineMetrics> _pipelineMetricsFactory = static () => NoOpPipelineMetrics.Instance;
    private IKafkaPipelineIntegrationMetrics _pipelineIntegrationMetrics = NoOpKafkaPipelineIntegrationMetrics.Instance;
    private IKafkaConsumerLifecycleObserver? _lifecycleObserver;
    private string _pipelineName = "kafka-consumer-pipeline";

    public KafkaConsumerHostBuilder UseKafkaConfigurationProfile(string profileName, IKafkaClientConfigFactory factory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);
        _configFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        _profileName = profileName;
        return this;
    }

    public KafkaConsumerHostBuilder UseProcessor<TProcessor>()
        where TProcessor : IKafkaBatchProcessor, new()
    {
        _processorFactory = () => new TProcessor();
        return this;
    }

    public KafkaConsumerHostBuilder UseProcessor(Func<IKafkaBatchProcessor> factory)
    {
        _processorFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaConsumerHostBuilder UseProcessingPipeline<TOut>(
        Func<IProcessingPipeline<KafkaPipelineRecord, TOut>> pipelineFactory,
        Action<KafkaPipelineProcessorBuilder<TOut>>? configure = null)
    {
        if (pipelineFactory is null)
        {
            throw new ArgumentNullException(nameof(pipelineFactory));
        }

        var builder = new KafkaPipelineProcessorBuilder<TOut>()
            .UsePipelineName(_pipelineName)
            .UsePipelineMetrics(_pipelineMetricsFactory)
            .UseIntegrationMetrics(_pipelineIntegrationMetrics);
        configure?.Invoke(builder);
        _processorFactory = () => builder.Build(pipelineFactory);
        // Capture a reference so Build() can apply ExtendedConsumerSettings after
        // the consumer profile (and its settings) are resolved from config.
        _consumerSettingsApplicator = settings => builder.ApplyConsumerSettings(settings);
        return this;
    }

    public KafkaConsumerHostBuilder UseLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        return this;
    }

    public KafkaConsumerHostBuilder UseBackpressurePolicy(IBackpressurePolicy policy)
    {
        _backpressurePolicy = policy ?? throw new ArgumentNullException(nameof(policy));
        return this;
    }

    public KafkaConsumerHostBuilder UseRestartPolicy(IRestartPolicy policy)
    {
        _restartPolicy = policy ?? throw new ArgumentNullException(nameof(policy));
        return this;
    }

    public KafkaConsumerHostBuilder UseConsumerLifecycleObserver(IKafkaConsumerLifecycleObserver observer)
    {
        _lifecycleObserver = observer ?? throw new ArgumentNullException(nameof(observer));
        return this;
    }

    public KafkaConsumerHostBuilder UseClock(ISystemClock clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }

    public KafkaConsumerHostBuilder UsePipelineName(string pipelineName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);
        _pipelineName = pipelineName;
        return this;
    }

    public KafkaConsumerHostBuilder UsePipelineMetrics(IPipelineMetrics metrics)
    {
        if (metrics is null)
        {
            throw new ArgumentNullException(nameof(metrics));
        }

        _pipelineMetricsFactory = () => metrics;
        return this;
    }

    public KafkaConsumerHostBuilder UsePipelineMetrics(Func<IPipelineMetrics> factory)
    {
        _pipelineMetricsFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaConsumerHostBuilder UsePipelineIntegrationMetrics(IKafkaPipelineIntegrationMetrics metrics)
    {
        _pipelineIntegrationMetrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        return this;
    }

    public KafkaConsumerHostBuilder UseOperationMonitor(IOperationMonitor monitor)
    {
        if (monitor is null)
        {
            throw new ArgumentNullException(nameof(monitor));
        }

        var metrics = new KafkaPipelineOperationMonitorMetrics(monitor);
        _pipelineMetricsFactory = () => metrics;
        _pipelineIntegrationMetrics = metrics;
        return this;
    }

    public KafkaConsumerHost Build()
    {
        if (_profileName is null)
        {
            throw new InvalidOperationException("Kafka profile was not configured.");
        }

        if (_configFactory is null)
        {
            throw new InvalidOperationException("Kafka configuration factory was not provided.");
        }

        if (_processorFactory is null)
        {
            throw new InvalidOperationException("Batch processor factory was not configured.");
        }

        var runtime = _configFactory.CreateConsumer(_profileName);

        // Apply consumer settings to the pipeline builder (e.g. DOP from config)
        // before creating the supervisor — the processor factory captures the builder.
        _consumerSettingsApplicator?.Invoke(runtime.Extended);

        var backpressurePolicy = _backpressurePolicy ?? CreateBackpressurePolicy(runtime.Extended, _loggerFactory);
        var runtimeState = new KafkaConsumerRuntimeState(runtime.Extended.ConsumerCount);
        var supervisor = new KafkaConsumerSupervisor(
            runtime,
            _processorFactory,
            runtimeState,
            _loggerFactory,
            _clock,
            backpressurePolicy,
            _restartPolicy,
            lifecycleObserver: _lifecycleObserver);
        return new KafkaConsumerHost(supervisor, runtimeState, _loggerFactory);
    }

    private static IBackpressurePolicy CreateBackpressurePolicy(ExtendedConsumerSettings settings, ILoggerFactory loggerFactory)
    {
        if (settings is null)
        {
            throw new ArgumentNullException(nameof(settings));
        }

        var configured = settings.Backpressure ?? new ConsumerBackpressureSettings();
        if (configured.PauseUtilization is < 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(ConsumerBackpressureSettings.PauseUtilization), "PauseUtilization must be between 0 and 1.");
        }

        if (configured.ResumeUtilization is < 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(ConsumerBackpressureSettings.ResumeUtilization), "ResumeUtilization must be between 0 and 1.");
        }

        if (configured.ResumeUtilization >= configured.PauseUtilization)
        {
            throw new ArgumentException("ResumeUtilization must be lower than PauseUtilization.", nameof(ConsumerBackpressureSettings));
        }

        var basePolicy = new ThresholdBackpressurePolicy(configured.PauseUtilization, configured.ResumeUtilization);
        if (!configured.Coordinated)
        {
            return basePolicy;
        }

        var logger = loggerFactory?.CreateLogger<CoordinatedBackpressurePolicy>();
        return new CoordinatedBackpressurePolicy(basePolicy, logger);
    }
}

public sealed class KafkaConsumerHost : IAsyncDisposable
{
    private readonly KafkaConsumerSupervisor _supervisor;
    private readonly KafkaConsumerRuntimeState _runtimeState;
    private readonly HostLogger<KafkaConsumerHost> _logs;
    private bool _started;

    internal KafkaConsumerHost(
        KafkaConsumerSupervisor supervisor,
        KafkaConsumerRuntimeState runtimeState,
        ILoggerFactory loggerFactory)
    {
        _supervisor = supervisor ?? throw new ArgumentNullException(nameof(supervisor));
        _runtimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
        var logger = loggerFactory.CreateLogger<KafkaConsumerHost>();
        _logs = new HostLogger<KafkaConsumerHost>(logger);
    }

    public KafkaConsumerRuntimeState RuntimeState => _runtimeState;

    public static KafkaConsumerHostBuilder Create() => new();

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_started)
        {
            throw new InvalidOperationException("Kafka consumer host already started.");
        }

        _logs.Status.Starting.LogInformation("Kafka consumer host starting");
        await _supervisor.StartAsync(cancellationToken);
        _started = true;
        _logs.Status.Started.LogInformation("Kafka consumer host started with {WorkerCount} workers", _supervisor.WorkerCount);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_started)
        {
            return;
        }

        _logs.Status.Stopping.LogInformation("Kafka consumer host stopping");
        await _supervisor.StopAsync(cancellationToken);
        _started = false;
        _logs.Status.Stopped.LogInformation("Kafka consumer host stopped");
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _supervisor.DisposeAsync();
    }
}
