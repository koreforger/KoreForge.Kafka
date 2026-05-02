using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KoreForge.Kafka.Configuration.Runtime;
using KoreForge.Kafka.Producer.Backlog;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Alerts;
using KoreForge.Kafka.Producer.Backpressure;
using KoreForge.Kafka.Producer.Buffer;
using KoreForge.Kafka.Producer.Logging;
using KoreForge.Kafka.Producer.Metrics;
using KoreForge.Kafka.Producer.Pipelines;
using KoreForge.Kafka.Producer.Restart;
using KoreForge.Kafka.Producer.Runtime;
using KoreForge.Kafka.Producer.Workers;
using KoreForge.Time;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Producer.Hosting;

internal sealed class KafkaProducerSupervisor : IAsyncDisposable
{
    private readonly KafkaProducerRuntimeConfig _runtimeConfig;
    private readonly IReadOnlyList<ProducerTopicDefinition> _topics;
    private readonly Dictionary<string, TopicPipeline> _pipelines;
    private readonly ProducerRuntimeState _runtimeState;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<KafkaProducerSupervisor> _logger;
    private readonly IProducerSerializerRegistry _serializerRegistry;
    private readonly IProducerBackpressurePolicy _backpressurePolicy;
    private readonly IProducerRestartPolicy _restartPolicy;
    private readonly IProducerBacklogPersistenceStrategy _backlog;
    private readonly ProducerMetricsBridge _metrics;
    private readonly ISystemClock _clock;
    private readonly ProducerAlertRegistry _alerts;
    private readonly CancellationTokenSource _stopCts = new();
    private readonly List<ProducerWorkerHandle> _workerHandles = new();
    private readonly List<Task> _observerTasks = new();
    private readonly TimeSpan _alertInterval;
    private readonly ILogger _bufferLogger;
    private readonly Func<ProducerConfig, IProducer<byte[], byte[]>> _producerFactory;
    private readonly IProducerBacklogDiagnostics? _backlogDiagnostics;

    private Task? _alertLoop;
    private bool _started;
    private bool _circuitBreakerTripped;

    public KafkaProducerSupervisor(
        KafkaProducerRuntimeConfig runtimeConfig,
        IProducerSerializerRegistry serializerRegistry,
        IProducerBackpressurePolicy backpressurePolicy,
        IProducerRestartPolicy restartPolicy,
        IProducerBacklogPersistenceStrategy backlog,
        ProducerMetricsBridge metrics,
        ISystemClock clock,
        ProducerAlertRegistry alerts,
        ILoggerFactory loggerFactory,
        Func<ProducerConfig, IProducer<byte[], byte[]>>? producerFactory = null)
    {
        _runtimeConfig = runtimeConfig ?? throw new ArgumentNullException(nameof(runtimeConfig));
        _serializerRegistry = serializerRegistry ?? throw new ArgumentNullException(nameof(serializerRegistry));
        _backpressurePolicy = backpressurePolicy ?? throw new ArgumentNullException(nameof(backpressurePolicy));
        _restartPolicy = restartPolicy ?? throw new ArgumentNullException(nameof(restartPolicy));
        _backlog = backlog ?? throw new ArgumentNullException(nameof(backlog));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _alerts = alerts ?? throw new ArgumentNullException(nameof(alerts));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = _loggerFactory.CreateLogger<KafkaProducerSupervisor>();
        _bufferLogger = _loggerFactory.CreateLogger<ProducerBuffer>();
        _producerFactory = producerFactory ?? CreateProducer;
        _backlogDiagnostics = backlog as IProducerBacklogDiagnostics;

        _topics = ProducerTopologyBuilder.BuildTopics(runtimeConfig);
        _pipelines = new Dictionary<string, TopicPipeline>(StringComparer.OrdinalIgnoreCase);

        var workerCount = 0;
        foreach (var topic in _topics)
        {
            var pipeline = new TopicPipeline(topic);
            _pipelines[topic.TopicName] = pipeline;
            workerCount += topic.WorkerCount;
        }

        _runtimeState = new ProducerRuntimeState(workerCount);
        Buffer = new ProducerBuffer(_pipelines, _backpressurePolicy, _metrics, _clock, _bufferLogger);
        _alertInterval = runtimeConfig.Extended.Alerts?.EvaluationInterval ?? TimeSpan.FromSeconds(5);
    }

    public IProducerBuffer Buffer { get; }
    public ProducerRuntimeState RuntimeState => _runtimeState;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_started)
        {
            throw new InvalidOperationException("Kafka producer supervisor already started.");
        }

        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Starting), "Starting Kafka producer supervisor with {TopicCount} topics", _topics.Count);
        await RestoreBacklogAsync(cancellationToken).ConfigureAwait(false);
        StartWorkers(cancellationToken);
        _alertLoop = Task.Run(() => RunAlertLoopAsync(_stopCts.Token), CancellationToken.None);
        _started = true;
        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Started), "Kafka producer supervisor started");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_started)
        {
            return;
        }

        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Stopping), "Stopping Kafka producer supervisor");
        _stopCts.Cancel();

        foreach (var handle in _workerHandles)
        {
            handle.IsStopping = true;
            await DisposeWorkerAsync(handle).ConfigureAwait(false);
        }

        if (_alertLoop is not null)
        {
            await _alertLoop.ConfigureAwait(false);
        }

        await Task.WhenAll(_observerTasks).ConfigureAwait(false);
        await PersistBacklogAsync(cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(ToEventId(ProducerLogEvents.Host_Status_Stopped), "Kafka producer supervisor stopped");
        _started = false;
    }

    private void StartWorkers(CancellationToken cancellationToken)
    {
        var workerIndex = 0;
        foreach (var topic in _topics)
        {
            var pipeline = _pipelines[topic.TopicName];
            _logger.LogInformation(ToEventId(ProducerLogEvents.Topic_Status_Starting), "Starting {WorkerCount} workers for topic {Topic}", topic.WorkerCount, topic.TopicName);
            for (var i = 0; i < topic.WorkerCount; i++)
            {
                var runtimeRecord = _runtimeState[workerIndex++];
                var handle = new ProducerWorkerHandle(i, topic.TopicName, runtimeRecord);
                handle.Worker = CreateWorker(handle, pipeline, cancellationToken);
                pipeline.Workers.Add(handle);
                _workerHandles.Add(handle);
                _observerTasks.Add(ObserveWorkerAsync(handle));
            }
            _logger.LogInformation(ToEventId(ProducerLogEvents.Topic_Status_Started), "Topic {Topic} workers running", topic.TopicName);
        }
    }

    private ProducerWorker CreateWorker(ProducerWorkerHandle handle, TopicPipeline pipeline, CancellationToken cancellationToken)
    {
        var config = BuildWorkerConfig(handle.WorkerId);
        var producer = _producerFactory(config);
        var workerLogger = _loggerFactory.CreateLogger<ProducerWorker>();
        var worker = new ProducerWorker(
            handle.WorkerId,
            pipeline.Definition.TopicName,
            pipeline.Channel.Reader,
            pipeline.Buffer,
            _serializerRegistry,
            handle.RuntimeRecord,
            producer,
            workerLogger,
            _metrics,
            _clock);
        _ = worker.StartAsync(cancellationToken);
        return worker;
    }

    private static IProducer<byte[], byte[]> CreateProducer(ProducerConfig config)
    {
        return new ProducerBuilder<byte[], byte[]>(config).Build();
    }

    private ProducerConfig BuildWorkerConfig(int workerId)
    {
        var config = new ProducerConfig(_runtimeConfig.ConfluentConfig);
        config.ClientId = BuildClientId(config.ClientId, workerId);
        return config;
    }

    private static string BuildClientId(string? baseClientId, int workerId)
    {
        var prefix = string.IsNullOrWhiteSpace(baseClientId) ? "koreforge-producer" : baseClientId;
        return $"{prefix}-w{workerId + 1}";
    }

    private async Task ObserveWorkerAsync(ProducerWorkerHandle handle)
    {
        while (!_stopCts.IsCancellationRequested && !handle.IsStopping)
        {
            try
            {
                await handle.Worker.Execution.ConfigureAwait(false);
                if (_stopCts.IsCancellationRequested)
                {
                    break;
                }

                handle.RuntimeRecord.RecordStopped();
                break;
            }
            catch (Exception ex)
            {
                if (_stopCts.IsCancellationRequested)
                {
                    break;
                }

                await HandleWorkerFailureAsync(handle, ex).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleWorkerFailureAsync(ProducerWorkerHandle handle, Exception exception)
    {
        var failureCount = handle.IncrementFailures();
        handle.RuntimeRecord.RecordFailure();
        var context = new ProducerFailureContext(handle.TopicName, handle.WorkerId, failureCount, exception, _clock.UtcNow);
        var decision = _restartPolicy.Evaluate(context);
        using var restartScope = _metrics.TrackRestartDecision(handle.TopicName, DescribeRestartDecision(decision));

        if (decision.IsCircuitBreakerTripped)
        {
            _circuitBreakerTripped = true;
            _logger.LogCritical(ToEventId(ProducerLogEvents.Restart_CircuitBreaker), "Circuit breaker tripped for topic {Topic}", handle.TopicName);
            handle.CircuitBreakerTripped = true;
            return;
        }

        if (!decision.ShouldRestart)
        {
            _logger.LogError(ToEventId(ProducerLogEvents.Restart_GiveUp), "Worker {WorkerId} will not be restarted", handle.WorkerId);
            return;
        }

        _logger.LogWarning(ToEventId(ProducerLogEvents.Restart_Scheduled), "Worker {Worker} restarting in {Delay}", handle.WorkerId, decision.DelayBeforeRestart);
        await DisposeWorkerAsync(handle).ConfigureAwait(false);

        if (decision.DelayBeforeRestart > TimeSpan.Zero)
        {
            try
            {
                await Task.Delay(decision.DelayBeforeRestart, _stopCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }

        if (_stopCts.IsCancellationRequested)
        {
            return;
        }

        handle.ResetFailures();
        var pipeline = _pipelines[handle.TopicName];
        handle.Worker = CreateWorker(handle, pipeline, _stopCts.Token);
    }

    private async Task DisposeWorkerAsync(ProducerWorkerHandle handle)
    {
        try
        {
            await handle.Worker.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to dispose producer worker {Worker}", handle.WorkerId);
        }
    }

    private async Task RestoreBacklogAsync(CancellationToken cancellationToken)
    {
        foreach (var pipeline in _pipelines.Values)
        {
            var settings = pipeline.Definition.BacklogSettings;
            if (!settings.ReplayEnabled)
            {
                continue;
            }

            var context = new ProducerBacklogRestoreContext(pipeline.Definition.TopicName, settings);
            _logger.LogInformation(ToEventId(ProducerLogEvents.Backlog_Restoring), "Restoring backlog for topic {Topic}", pipeline.Definition.TopicName);
            var envelopes = await _backlog.RestoreAsync(context, cancellationToken).ConfigureAwait(false);
            if (envelopes.Count == 0)
            {
                continue;
            }

            var scope = _metrics.TrackBacklogRestore(pipeline.Definition.TopicName, envelopes.Count);
            try
            {
                foreach (var envelope in envelopes)
                {
                    await pipeline.Channel.Writer.WriteAsync(envelope, cancellationToken).ConfigureAwait(false);
                    pipeline.Buffer.Increment();
                }

                _logger.LogInformation(ToEventId(ProducerLogEvents.Backlog_Restored), "Restored {Count} backlog envelopes for topic {Topic}", envelopes.Count, pipeline.Definition.TopicName);
            }
            catch
            {
                scope.MarkFailed();
                throw;
            }
            finally
            {
                scope.Dispose();
            }
        }
    }

    private async Task PersistBacklogAsync(CancellationToken cancellationToken)
    {
        foreach (var pipeline in _pipelines.Values)
        {
            var settings = pipeline.Definition.BacklogSettings;
            if (!settings.PersistenceEnabled)
            {
                continue;
            }

            var drained = Drain(pipeline);
            var context = new ProducerBacklogPersistContext(pipeline.Definition.TopicName, drained, settings);
            if (drained.Count == 0)
            {
                await _backlog.PersistAsync(context, cancellationToken).ConfigureAwait(false);
                continue;
            }

            _logger.LogInformation(ToEventId(ProducerLogEvents.Backlog_Persisting), "Persisting {Count} backlog envelopes for topic {Topic}", drained.Count, pipeline.Definition.TopicName);
            var scope = _metrics.TrackBacklogPersist(pipeline.Definition.TopicName, drained.Count);
            try
            {
                await _backlog.PersistAsync(context, cancellationToken).ConfigureAwait(false);
                _logger.LogInformation(ToEventId(ProducerLogEvents.Backlog_Persisted), "Persisted {Count} backlog envelopes for topic {Topic}", drained.Count, pipeline.Definition.TopicName);
            }
            catch
            {
                scope.MarkFailed();
                throw;
            }
            finally
            {
                scope.Dispose();
            }
        }
    }

    private static IReadOnlyList<OutgoingEnvelope> Drain(TopicPipeline pipeline)
    {
        var envelopes = new List<OutgoingEnvelope>();
        while (pipeline.Channel.Reader.TryRead(out var envelope))
        {
            pipeline.Buffer.Decrement();
            envelopes.Add(envelope);
        }

        return envelopes;
    }

    private static string DescribeRestartDecision(ProducerRestartDecision decision)
    {
        if (decision.IsCircuitBreakerTripped)
        {
            return "circuit-breaker";
        }

        return decision.ShouldRestart ? "restart" : "give-up";
    }

    private async Task RunAlertLoopAsync(CancellationToken cancellationToken)
    {
        if (_alertInterval <= TimeSpan.Zero || _alerts.Alerts.Count == 0)
        {
            return;
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var snapshot = BuildSnapshot();
                _alerts.Evaluate(snapshot);
                await Task.Delay(_alertInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private ProducerStatusSnapshot BuildSnapshot()
    {
        var topicStats = _pipelines.Values.Select(p => new TopicBufferStatus
        {
            TopicName = p.Definition.TopicName,
            BufferedMessages = p.Buffer.Buffered,
            BufferCapacity = p.Definition.ChannelCapacity,
            Backlog = _backlogDiagnostics?.GetTopicStatus(p.Definition.TopicName)
        }).ToList();

        return new ProducerStatusSnapshot
        {
            TotalWorkers = _runtimeState.WorkerCount,
            TotalBufferedMessages = topicStats.Sum(t => t.BufferedMessages),
            TotalBufferCapacity = topicStats.Sum(t => t.BufferCapacity),
            CircuitBreakerTripped = _circuitBreakerTripped,
            Topics = topicStats,
            RuntimeRecords = _runtimeState.Snapshot()
        };
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _stopCts.Dispose();
    }

    private static EventId ToEventId(ProducerLogEvents evt) => new((int)evt, evt.ToString());
}
