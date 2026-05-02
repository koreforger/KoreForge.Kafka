using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Runtime;
using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Backpressure;
using KoreForge.Kafka.Consumer.Logging;
using KoreForge.Kafka.Consumer.Restart;
using KoreForge.Kafka.Consumer.Runtime;
using KoreForge.Time;
using KoreForge.Kafka.Core.Runtime;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Consumer.Execution;

internal sealed class KafkaConsumerSupervisor : IAsyncDisposable
{
    private readonly List<WorkerHandle> _workerHandles = new();
    private readonly CancellationTokenSource _stopCts = new();
    private readonly KafkaConsumerRuntimeConfig _runtimeConfig;
    private readonly Func<IKafkaBatchProcessor> _processorFactory;
    private readonly KafkaConsumerRuntimeState _runtimeState;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<KafkaConsumerSupervisor> _logger;
    private readonly HostLogger<KafkaConsumerSupervisor> _hostLogs;
    private readonly WorkerLogger<KafkaConsumerSupervisor> _workerLogs;
    private readonly CertificateWorkspace? _certificateWorkspace;
    private readonly List<Task> _observerTasks = new();
    private readonly ISystemClock _clock;
    private readonly IBackpressurePolicy _backpressurePolicy;
    private readonly IRestartPolicy _restartPolicy;
    private readonly IKafkaConsumerLifecycleObserver? _lifecycleObserver;
    private readonly Func<KafkaConsumerWorkerCreationContext, IKafkaConsumerWorker> _workerFactory;
    private bool _started;

    public KafkaConsumerSupervisor(
        KafkaConsumerRuntimeConfig runtimeConfig,
        Func<IKafkaBatchProcessor> processorFactory,
        KafkaConsumerRuntimeState runtimeState,
        ILoggerFactory loggerFactory,
        ISystemClock clock,
        IBackpressurePolicy backpressurePolicy,
        IRestartPolicy restartPolicy,
        Func<KafkaConsumerWorkerCreationContext, IKafkaConsumerWorker>? workerFactory = null,
        IKafkaConsumerLifecycleObserver? lifecycleObserver = null)
    {
        _runtimeConfig = runtimeConfig ?? throw new ArgumentNullException(nameof(runtimeConfig));
        _processorFactory = processorFactory ?? throw new ArgumentNullException(nameof(processorFactory));
        _runtimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _clock = clock ?? SystemClock.Instance;
        _backpressurePolicy = backpressurePolicy ?? throw new ArgumentNullException(nameof(backpressurePolicy));
        _restartPolicy = restartPolicy ?? throw new ArgumentNullException(nameof(restartPolicy));
        _lifecycleObserver = lifecycleObserver;
        _workerFactory = workerFactory ?? CreateWorker;

        var supervisorLogger = _loggerFactory.CreateLogger<KafkaConsumerSupervisor>();
        _logger = supervisorLogger;
        _hostLogs = new HostLogger<KafkaConsumerSupervisor>(supervisorLogger);
        _workerLogs = new WorkerLogger<KafkaConsumerSupervisor>(supervisorLogger);

        if (_runtimeConfig.Extended.IsolateConsumerCertificatePerThread && _runtimeConfig.Security.Mode == KafkaSecurityMode.Certificates)
        {
            var certLogger = _loggerFactory.CreateLogger<CertificateWorkspace>();
            _certificateWorkspace = new CertificateWorkspace(_runtimeConfig.Security, _runtimeConfig.ConfluentConfig, _runtimeConfig.Extended, certLogger);
        }

        var workerCount = _runtimeConfig.Extended.ConsumerCount;
        for (var i = 0; i < workerCount; i++)
        {
            var handle = new WorkerHandle(i, _runtimeState[i]);
            var workerContext = CreateWorkerContext(handle);
            handle.Worker = _workerFactory(workerContext);
            _workerHandles.Add(handle);
        }
    }

    public int WorkerCount => _workerHandles.Count;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_started)
        {
            throw new InvalidOperationException("Supervisor already started.");
        }

        _hostLogs.Status.Starting.LogInformation("Starting {WorkerCount} Kafka consumer workers", WorkerCount);
        foreach (var handle in _workerHandles)
        {
            await StartWorkerAsync(handle);
        }

        _started = true;
        _hostLogs.Status.Started.LogInformation("Kafka consumer workers started");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_started)
        {
            return;
        }

        _hostLogs.Status.Stopping.LogInformation("Stopping Kafka consumer workers");
        _stopCts.Cancel();

        foreach (var handle in _workerHandles)
        {
            handle.IsStopping = true;
            if (!handle.WorkerDisposed)
            {
                await handle.Worker.StopAsync(cancellationToken);
            }
        }

        await Task.WhenAll(_observerTasks);
        _observerTasks.Clear();
        _hostLogs.Status.Stopped.LogInformation("Kafka consumer workers stopped");
        _started = false;
    }

    private async Task StartWorkerAsync(WorkerHandle handle)
    {
        await handle.Worker.StartAsync(_stopCts.Token);
        handle.WorkerDisposed = false;
        _observerTasks.Add(ObserveWorkerAsync(handle));
    }

    private async Task ObserveWorkerAsync(WorkerHandle handle)
    {
        while (!_stopCts.IsCancellationRequested && !handle.IsStopping)
        {
            try
            {
                await handle.Worker.Execution;

                if (_stopCts.IsCancellationRequested || handle.IsStopping)
                {
                    break;
                }

                // Worker completed unexpectedly without cancellation; treat as permanent stop.
                handle.RuntimeRecord.UpdateState(ConsumerWorkerState.Stopped);
                break;
            }
            catch (Exception ex)
            {
                if (_stopCts.IsCancellationRequested || handle.IsStopping)
                {
                    break;
                }

                await HandleWorkerFailureAsync(handle, ex).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleWorkerFailureAsync(WorkerHandle handle, Exception exception)
    {
        handle.IncrementFailures();
        _workerLogs.Batch.Failed.LogError(exception, "Worker {WorkerId} faulted", handle.Worker.WorkerId);
        handle.RuntimeRecord.UpdateState(ConsumerWorkerState.Failed);

        var context = new RestartContext(
            handle.Worker.WorkerId,
            handle.ConsecutiveFailures,
            exception,
            _clock.UtcNow);

        var decision = _restartPolicy.Evaluate(context);
        if (decision.Action == RestartAction.Restart)
        {
            _logger.LogWarning(
                ToEventId(ConsumerLogEvents.Worker_Restart_Scheduled),
                "Worker {WorkerId} restarting in {Delay} after {FailureCount} failures",
                handle.Worker.WorkerId,
                decision.Delay,
                handle.ConsecutiveFailures);

            await DisposeWorkerAsync(handle).ConfigureAwait(false);
            handle.RuntimeRecord.UpdateState(ConsumerWorkerState.Failed);

            if (decision.Delay > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(decision.Delay, _stopCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }

            if (_stopCts.IsCancellationRequested || handle.IsStopping)
            {
                return;
            }

            var workerContext = CreateWorkerContext(handle);
            handle.Worker = _workerFactory(workerContext);
            handle.WorkerDisposed = false;
            await handle.Worker.StartAsync(_stopCts.Token).ConfigureAwait(false);
            return;
        }

        if (decision.Action == RestartAction.GiveUp || decision.Action == RestartAction.None)
        {
            _logger.LogCritical(
                ToEventId(ConsumerLogEvents.Worker_Restart_GiveUp),
                "Worker {WorkerId} permanently failed after {FailureCount} failures",
                handle.Worker.WorkerId,
                handle.ConsecutiveFailures);

            handle.RuntimeRecord.UpdateState(ConsumerWorkerState.Failed);
            handle.IsStopping = true;
            await DisposeWorkerAsync(handle).ConfigureAwait(false);
            handle.RuntimeRecord.UpdateState(ConsumerWorkerState.Failed);
        }
    }

    private async Task DisposeWorkerAsync(WorkerHandle handle)
    {
        if (handle.WorkerDisposed)
        {
            return;
        }

        handle.WorkerDisposed = true;
        try
        {
            await handle.Worker.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _workerLogs.Status.Stopping.LogWarning(ex, "Worker {WorkerId} disposal failed", handle.Worker.WorkerId);
        }
    }

    private KafkaConsumerWorkerCreationContext CreateWorkerContext(WorkerHandle handle) =>
        new(handle.Index, handle.RuntimeRecord, handle.BeginNewWorkerInstance, handle.ResetFailures);

    private IKafkaConsumerWorker CreateWorker(KafkaConsumerWorkerCreationContext context)
    {
        var processor = _processorFactory();
        var workerLogger = _loggerFactory.CreateLogger<KafkaConsumerWorker>();
        var workerConfig = CreateWorkerConfig(_runtimeConfig.ConfluentConfig, _runtimeConfig.Extended, context.WorkerIndex);
        _certificateWorkspace?.Apply(workerConfig, context.WorkerIndex);
        var instanceId = context.BeginNewWorkerInstance();

        return new KafkaConsumerWorker(
            context.WorkerIndex,
            _runtimeConfig.Topics,
            workerConfig,
            _runtimeConfig.Extended,
            processor,
            context.RuntimeRecord,
            workerLogger,
            _clock,
            _backpressurePolicy,
            _lifecycleObserver,
            () => context.ResetFailures(instanceId));
    }

    private static ConsumerConfig CreateWorkerConfig(ConsumerConfig template, ExtendedConsumerSettings settings, int workerIndex)
    {
        var config = new ConsumerConfig(template);
        config.ClientId = BuildClientId(template.ClientId, workerIndex);
        config.EnablePartitionEof = true;
        // Disable auto-store so that StoreOffset() in CompleteBatchAsync controls exactly
        // which offsets are eligible for the next periodic auto-commit.  This prevents
        // librdkafka from auto-storing every polled message before we have processed it.
        config.EnableAutoOffsetStore = false;
        return config;
    }

    private static string BuildClientId(string? baseClientId, int workerIndex)
    {
        var prefix = string.IsNullOrWhiteSpace(baseClientId) ? "koreforge-consumer" : baseClientId;
        return $"{prefix}-w{workerIndex + 1}";
    }

    public async ValueTask DisposeAsync()
    {
        _stopCts.Cancel();
        foreach (var handle in _workerHandles)
        {
            if (!handle.WorkerDisposed)
            {
                await handle.Worker.DisposeAsync();
            }
        }

        if (_certificateWorkspace is not null)
        {
            await _certificateWorkspace.DisposeAsync();
        }

        _stopCts.Dispose();
    }

    private static EventId ToEventId(ConsumerLogEvents logEvent) => new((int)logEvent, logEvent.ToString());

    private sealed class WorkerHandle
    {
        public WorkerHandle(int index, ConsumerRuntimeRecord runtimeRecord)
        {
            Index = index;
            RuntimeRecord = runtimeRecord;
        }

        private int _consecutiveFailures;
        private long _workerInstanceId;

        public int Index { get; }
        public IKafkaConsumerWorker Worker { get; set; } = null!;
        public ConsumerRuntimeRecord RuntimeRecord { get; }
        public bool IsStopping { get; set; }
        public bool WorkerDisposed { get; set; }

        public int ConsecutiveFailures => Volatile.Read(ref _consecutiveFailures);

        public void IncrementFailures() => Interlocked.Increment(ref _consecutiveFailures);

        public long BeginNewWorkerInstance() => Interlocked.Increment(ref _workerInstanceId);

        public void ResetFailures(long instanceId)
        {
            if (instanceId != Volatile.Read(ref _workerInstanceId))
            {
                return;
            }

            Volatile.Write(ref _consecutiveFailures, 0);
        }
    }
}
