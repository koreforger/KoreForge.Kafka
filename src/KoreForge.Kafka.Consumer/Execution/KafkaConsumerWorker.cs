using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Backpressure;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Kafka.Consumer.Logging;
using KoreForge.Time;
using KoreForge.Kafka.Core.Runtime;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Consumer.Execution;

internal sealed class KafkaConsumerWorker : IKafkaConsumerWorker
{
    private readonly int _workerIndex;
    private readonly IReadOnlyCollection<string> _topics;
    private readonly ConsumerConfig _config;
    private readonly ExtendedConsumerSettings _settings;
    private readonly IKafkaBatchProcessor _processor;
    private readonly ConsumerRuntimeRecord _runtimeRecord;
    private readonly WorkerLogger<KafkaConsumerWorker> _logs;
    private readonly ISystemClock _clock;
    private readonly IBackpressurePolicy _backpressurePolicy;
    private readonly IKafkaConsumerLifecycleObserver? _lifecycleObserver;
    private readonly List<PendingBatch> _pendingBatches = new();
    private readonly int _maxInFlightBatches;
    private readonly object _assignmentLock = new();
    private List<TopicPartition> _assignedPartitions = new();
    private bool _pauseRequested;
    private bool _isPaused;
    private CancellationTokenSource? _cts;
    private Task? _executionTask;
    private readonly Action? _onBatchSuccess;
    private Error? _fatalKafkaError;
    private long _backpressureEvaluationCount;
    private long _backpressurePauseDecisionCount;
    private long _backpressureResumeDecisionCount;
    private DateTimeOffset _nextBackpressureSummaryAt;

    public KafkaConsumerWorker(
        int workerIndex,
        IReadOnlyCollection<string> topics,
        ConsumerConfig config,
        ExtendedConsumerSettings settings,
        IKafkaBatchProcessor processor,
        ConsumerRuntimeRecord runtimeRecord,
        ILogger<KafkaConsumerWorker> logger,
        ISystemClock clock,
        IBackpressurePolicy backpressurePolicy,
        IKafkaConsumerLifecycleObserver? lifecycleObserver = null,
        Action? onBatchSuccess = null)
    {
        _workerIndex = workerIndex;
        _topics = topics ?? throw new ArgumentNullException(nameof(topics));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
        _runtimeRecord = runtimeRecord ?? throw new ArgumentNullException(nameof(runtimeRecord));
        _clock = clock ?? SystemClock.Instance;
        _backpressurePolicy = backpressurePolicy ?? throw new ArgumentNullException(nameof(backpressurePolicy));
        _lifecycleObserver = lifecycleObserver;
        _logs = new WorkerLogger<KafkaConsumerWorker>(logger ?? throw new ArgumentNullException(nameof(logger)));
        _maxInFlightBatches = settings.MaxInFlightBatches <= 0 ? 1 : settings.MaxInFlightBatches;
        _onBatchSuccess = onBatchSuccess;
        _runtimeRecord.RecordBacklog(0, _maxInFlightBatches);
    }

    public int WorkerId => _workerIndex + 1;

    public Task Execution => _executionTask ?? Task.CompletedTask;

    internal TestAccessor CreateTestAccessor() => new(this);

    public Task StartAsync(CancellationToken supervisorToken)
    {
        if (_executionTask is not null)
        {
            throw new InvalidOperationException("Worker already started.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(supervisorToken);
        _runtimeRecord.UpdateState(ConsumerWorkerState.Starting);
        _logs.Status.Starting.LogInformation("Worker {WorkerId} starting", WorkerId);
        _executionTask = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_executionTask is null)
        {
            return;
        }

        _logs.Status.Stopping.LogInformation("Worker {WorkerId} stopping", WorkerId);
        _cts?.Cancel();

        try
        {
            await _executionTask.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Host requested cancellation; swallow to allow graceful shutdown.
        }
        catch (Exception)
        {
            // Worker already faulted (e.g. processor threw); the supervisor has
            // already observed and handled the failure via the observer loop.
            // Swallow here so shutdown completes cleanly.
        }
        finally
        {
            _logs.Status.Stopped.LogInformation("Worker {WorkerId} stopped", WorkerId);
            _executionTask = null;
            _cts?.Dispose();
            _cts = null;
            _runtimeRecord.UpdateState(ConsumerWorkerState.Stopped);
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var consumer = BuildConsumer();
        try
        {
            consumer.Subscribe(_topics);

            var buffer = new List<ConsumeResult<byte[], byte[]>>(_settings.MaxBatchSize);
            var batchWindow = TimeSpan.FromMilliseconds(_settings.MaxBatchWaitMs);
            var nextFlushAt = _clock.Now + batchWindow;

            while (!cancellationToken.IsCancellationRequested)
            {
                await DrainCompletedBatchesAsync(consumer, waitForAll: false, cancellationToken);
                ThrowIfFatalKafkaError();

                ConsumeResult<byte[], byte[]>? result;
                try
                {
                    result = consumer.Consume(GetCurrentPollTimeout());
                }
                catch (ConsumeException ex)
                {
                    _logs.Consume.Error.LogWarning(ex, "Worker {WorkerId} consume error: {Reason}", WorkerId, ex.Error.Reason);
                    continue;
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                ThrowIfFatalKafkaError();

                if (result is null)
                {
                    nextFlushAt = await FlushIfDueAsync(buffer, consumer, nextFlushAt, batchWindow, cancellationToken);
                    continue;
                }

                if (result.IsPartitionEOF)
                {
                    continue;
                }

                buffer.Add(result);
                if (buffer.Count >= _settings.MaxBatchSize)
                {
                    await DispatchAsync(buffer, consumer, cancellationToken);
                    nextFlushAt = _clock.Now + batchWindow;
                    continue;
                }

                if (_clock.Now >= nextFlushAt)
                {
                    await DispatchAsync(buffer, consumer, cancellationToken);
                    nextFlushAt = _clock.Now + batchWindow;
                }
            }

            await DispatchAsync(buffer, consumer, CancellationToken.None);
            await DrainCompletedBatchesAsync(consumer, waitForAll: true, CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        catch (Exception ex)
        {
            _logs.Batch.Failed.LogError(ex, "Worker {WorkerId} terminated unexpectedly", WorkerId);
            throw;
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logs.Status.Stopping.LogWarning(ex, "Worker {WorkerId} failed to close consumer", WorkerId);
            }
        }
    }

    private async Task<DateTimeOffset> FlushIfDueAsync(
        List<ConsumeResult<byte[], byte[]>> buffer,
        IConsumer<byte[], byte[]> consumer,
        DateTimeOffset nextFlushAt,
        TimeSpan batchWindow,
        CancellationToken cancellationToken)
    {
        if (buffer.Count == 0)
        {
            return _clock.Now >= nextFlushAt ? _clock.Now + batchWindow : nextFlushAt;
        }

        if (_clock.Now >= nextFlushAt)
        {
            await DispatchAsync(buffer, consumer, cancellationToken);
            return _clock.Now + batchWindow;
        }

        return nextFlushAt;
    }

    private Task DispatchAsync(
        List<ConsumeResult<byte[], byte[]>> buffer,
        IConsumer<byte[], byte[]> consumer,
        CancellationToken cancellationToken)
    {
        if (buffer.Count == 0)
        {
            return Task.CompletedTask;
        }

        var snapshot = buffer.ToArray();
        buffer.Clear();

        var startedAt = _clock.Now;
        var batch = new KafkaRecordBatch(snapshot, startedAt);
        var processingTask = Task.Run(() => ProcessBatchWithTimeoutAsync(batch, cancellationToken), CancellationToken.None);
        _pendingBatches.Add(new PendingBatch(batch, processingTask, startedAt));
        _runtimeRecord.RecordBacklog(_pendingBatches.Count, _maxInFlightBatches);
        EvaluateBackpressure(consumer);
        return Task.CompletedTask;
    }

    private IConsumer<byte[], byte[]> BuildConsumer()
    {
        var builder = new ConsumerBuilder<byte[], byte[]>(_config)
            .SetErrorHandler((_, error) =>
            {
                if (error.IsFatal)
                {
                    _logs.Consume.Error.LogCritical("Worker {WorkerId} fatal Kafka error: {Reason}", WorkerId, error.Reason);
                    _fatalKafkaError = error;
                }
                else
                {
                    _logs.Consume.Error.LogWarning("Worker {WorkerId} Kafka error: {Reason}", WorkerId, error.Reason);
                }
            })
            .SetPartitionsAssignedHandler((consumer, partitions) => HandlePartitionsAssigned(consumer, partitions))
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logs.Status.Stopping.LogInformation("Worker {WorkerId} partitions revoked ({PartitionCount})", WorkerId, partitions.Count);
                NotifyPartitionsRevoked(partitions.Select(partition => partition.TopicPartition).ToArray());
                lock (_assignmentLock)
                {
                    _assignedPartitions = new List<TopicPartition>();
                }

                _isPaused = false;
                _runtimeRecord.UpdateState(ConsumerWorkerState.Starting);
            });

        return builder.Build();
    }

    private IEnumerable<TopicPartitionOffset> HandlePartitionsAssigned(
        IConsumer<byte[], byte[]> consumer,
        List<TopicPartition> partitions)
    {
        _logs.Status.Started.LogInformation("Worker {WorkerId} partitions assigned ({PartitionCount})", WorkerId, partitions.Count);
        NotifyPartitionsAssigned(partitions);
        lock (_assignmentLock)
        {
            _assignedPartitions = partitions.ToList();
        }

        if (_pauseRequested)
        {
            consumer.Pause(partitions);
            _isPaused = true;
            _runtimeRecord.UpdateState(ConsumerWorkerState.Paused);
        }
        else
        {
            _isPaused = false;
            _runtimeRecord.UpdateState(ConsumerWorkerState.Active);
        }
        _pauseRequested = false;

        if (_settings.StartMode == StartMode.RelativeTime)
        {
            return ResolveRelativeAssignments(consumer, partitions);
        }

        var offset = _settings.StartMode switch
        {
            StartMode.Latest => Offset.End,
            StartMode.Earliest => Offset.Beginning,
            _ => Offset.Stored
        };

        return partitions.Select(partition => new TopicPartitionOffset(partition, offset)).ToArray();
    }

    private void NotifyPartitionsAssigned(IReadOnlyList<TopicPartition> partitions)
    {
        if (_lifecycleObserver is null)
        {
            return;
        }

        try
        {
            _lifecycleObserver.OnPartitionsAssigned(new KafkaConsumerLifecycleEvent(WorkerId, partitions.ToArray(), _clock.UtcNow));
        }
        catch (Exception ex)
        {
            _logs.Status.Started.LogWarning(ex, "Worker {WorkerId} lifecycle observer failed while recording assigned partitions", WorkerId);
        }
    }

    private void NotifyPartitionsRevoked(IReadOnlyList<TopicPartition> partitions)
    {
        if (_lifecycleObserver is null)
        {
            return;
        }

        try
        {
            _lifecycleObserver.OnPartitionsRevoked(new KafkaConsumerLifecycleEvent(WorkerId, partitions.ToArray(), _clock.UtcNow));
        }
        catch (Exception ex)
        {
            _logs.Status.Stopping.LogWarning(ex, "Worker {WorkerId} lifecycle observer failed while recording revoked partitions", WorkerId);
        }
    }

    private IEnumerable<TopicPartitionOffset> ResolveRelativeAssignments(
        IConsumer<byte[], byte[]> consumer,
        List<TopicPartition> partitions)
    {
        if (_settings.StartRelative is null || _settings.StartRelative <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("Relative start mode requires a positive StartRelative value.");
        }

        var targetTimestamp = _clock.UtcNow - _settings.StartRelative.Value;
        var requests = partitions.Select(p => new TopicPartitionTimestamp(p, new Timestamp(targetTimestamp.UtcDateTime, TimestampType.CreateTime))).ToList();

        try
        {
            var offsets = consumer.OffsetsForTimes(requests, TimeSpan.FromSeconds(10));
            return offsets.Select((offset, index) =>
            {
                if (offset == null || offset.Offset < 0)
                {
                    return new TopicPartitionOffset(requests[index].TopicPartition, Offset.Beginning);
                }

                return offset;
            }).ToArray();
        }
        catch (KafkaException ex)
        {
            _logs.Assignment.Error.LogError(ex, "Worker {WorkerId} failed to resolve timestamp offsets", WorkerId);
            throw;
        }
    }

    private async Task DrainCompletedBatchesAsync(
        IConsumer<byte[], byte[]> consumer,
        bool waitForAll,
        CancellationToken cancellationToken)
    {
        var waitToken = waitForAll ? CancellationToken.None : cancellationToken;
        while (true)
        {
            var completed = _pendingBatches.Where(batch => batch.Task.IsCompleted).ToList();
            if (completed.Count == 0)
            {
                if (waitForAll && _pendingBatches.Count > 0)
                {
                    var next = _pendingBatches.Select(batch => batch.Task).ToArray();
                    await Task.WhenAny(next).WaitAsync(waitToken);
                    continue;
                }

                break;
            }

            foreach (var pending in completed)
            {
                await CompleteBatchAsync(pending, consumer).ConfigureAwait(false);
                _pendingBatches.Remove(pending);
            }

            _runtimeRecord.RecordBacklog(_pendingBatches.Count, _maxInFlightBatches);
            EvaluateBackpressure(consumer);

            if (!waitForAll)
            {
                break;
            }
        }
    }

    private async Task CompleteBatchAsync(PendingBatch pending, IConsumer<byte[], byte[]> consumer)
    {
        try
        {
            await pending.Task.ConfigureAwait(false);
            var duration = _clock.Now - pending.StartedAt;
            _runtimeRecord.RecordBatch(pending.Batch.Count, duration, _clock.Now.LocalDateTime);
            // StoreOffset is a non-blocking, local operation.  librdkafka will include this
            // offset in the next periodic auto-commit (enable.auto.commit = true, default).
            // Using the synchronous Commit() here caused a ~50-70 ms broker round-trip that
            // blocked the consumer loop on every single batch.
            consumer.StoreOffset(pending.Batch.Records[^1]);
            _onBatchSuccess?.Invoke();
            _logs.Batch.Dispatched.LogDebug("Worker {WorkerId} processed batch of {BatchSize}", WorkerId, pending.Batch.Count);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (KafkaException ex)
        {
            _logs.Batch.Failed.LogError(ex, "Worker {WorkerId} failed to commit offsets: {Reason}", WorkerId, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logs.Batch.Failed.LogError(ex, "Worker {WorkerId} processor failure for batch of {BatchSize}", WorkerId, pending.Batch.Count);
            throw;
        }
    }

    private void EvaluateBackpressure(IConsumer<byte[], byte[]> consumer)
    {
        var context = new BackpressureContext(
            WorkerId,
            _pendingBatches.Count,
            _maxInFlightBatches,
            _pendingBatches.Count == 0 ? 0 : (double)_pendingBatches.Count / _maxInFlightBatches);

        var decision = _backpressurePolicy.Evaluate(context);
        RecordBackpressureDecision(decision);
        LogBackpressureSummaryIfDue(context);
        if (decision == BackpressureDecision.Pause)
        {
            PauseConsumer(consumer);
        }
        else if (decision == BackpressureDecision.Resume)
        {
            ResumeConsumer(consumer);
        }
    }

    private void PauseConsumer(IConsumer<byte[], byte[]> consumer)
    {
        if (_isPaused)
        {
            return;
        }

        var partitions = GetAssignedPartitions();
        if (partitions.Count == 0)
        {
            _pauseRequested = true;
            _logs.Backpressure.Deferred.LogInformation(
                "Worker {WorkerId} requested pause without assignments (backlog {Backlog}/{Capacity})",
                WorkerId,
                _pendingBatches.Count,
                _maxInFlightBatches);
            return;
        }

        consumer.Pause(partitions);
        _isPaused = true;
        _pauseRequested = false;
        _runtimeRecord.UpdateState(ConsumerWorkerState.Paused);
        _logs.Backpressure.Paused.LogInformation(
            "Worker {WorkerId} paused due to backpressure with backlog {Backlog}/{Capacity} ({Utilization:P0}) across {PartitionCount} partitions",
            WorkerId,
            _pendingBatches.Count,
            _maxInFlightBatches,
            CurrentUtilization(),
            partitions.Count);
    }

    private void ResumeConsumer(IConsumer<byte[], byte[]> consumer)
    {
        if (!_isPaused && !_pauseRequested)
        {
            return;
        }

        var partitions = GetAssignedPartitions();
        if (partitions.Count == 0)
        {
            _pauseRequested = false;
            _isPaused = false;
            _logs.Backpressure.Deferred.LogInformation(
                "Worker {WorkerId} cleared pause request without assignments",
                WorkerId);
            return;
        }

        consumer.Resume(partitions);
        _isPaused = false;
        _pauseRequested = false;
        _runtimeRecord.UpdateState(ConsumerWorkerState.Active);
        _logs.Backpressure.Resumed.LogInformation(
            "Worker {WorkerId} resumed consumption with backlog {Backlog}/{Capacity} ({Utilization:P0})",
            WorkerId,
            _pendingBatches.Count,
            _maxInFlightBatches,
            CurrentUtilization());
    }

    private void RecordBackpressureDecision(BackpressureDecision decision)
    {
        _backpressureEvaluationCount++;
        if (decision == BackpressureDecision.Pause)
        {
            _backpressurePauseDecisionCount++;
        }
        else if (decision == BackpressureDecision.Resume)
        {
            _backpressureResumeDecisionCount++;
        }
    }

    private void LogBackpressureSummaryIfDue(BackpressureContext context)
    {
        if (_settings.BackpressureSummaryLogIntervalMs <= 0)
        {
            return;
        }

        var now = _clock.Now;
        if (_nextBackpressureSummaryAt == default)
        {
            _nextBackpressureSummaryAt = now + TimeSpan.FromMilliseconds(_settings.BackpressureSummaryLogIntervalMs);
            return;
        }

        if (now < _nextBackpressureSummaryAt)
        {
            return;
        }

        _logs.Backpressure.Summary.LogInformation(
            "Worker {WorkerId} backpressure summary: evaluated {EvaluationCount} times, pause decisions {PauseDecisionCount}, resume decisions {ResumeDecisionCount}, current backlog {Backlog}/{Capacity} ({Utilization:P0}), paused {IsPaused}",
            context.WorkerId,
            _backpressureEvaluationCount,
            _backpressurePauseDecisionCount,
            _backpressureResumeDecisionCount,
            context.BacklogSize,
            context.Capacity,
            context.Utilization,
            _isPaused);

        _backpressureEvaluationCount = 0;
        _backpressurePauseDecisionCount = 0;
        _backpressureResumeDecisionCount = 0;
        _nextBackpressureSummaryAt = now + TimeSpan.FromMilliseconds(_settings.BackpressureSummaryLogIntervalMs);
    }

    private TimeSpan GetCurrentPollTimeout()
    {
        var timeoutMs = _isPaused ? _settings.PausedPollTimeoutMs : _settings.PollTimeoutMs;
        return TimeSpan.FromMilliseconds(timeoutMs <= 0 ? 1 : timeoutMs);
    }

    private void ThrowIfFatalKafkaError()
    {
        if (_fatalKafkaError is not null)
        {
            throw new KafkaFatalConsumerException(WorkerId, _fatalKafkaError);
        }
    }

    private async Task ProcessBatchWithTimeoutAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromMilliseconds(_settings.BatchProcessingTimeoutMs <= 0 ? 1 : _settings.BatchProcessingTimeoutMs);
        using var timeoutCts = _settings.BatchTimeoutAction == ConsumerBatchTimeoutAction.CancelBatchAndFailWorker
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        var processorToken = timeoutCts?.Token ?? cancellationToken;
        var processorTask = _processor.ProcessAsync(batch, processorToken);
        var delayTask = Task.Delay(timeout, cancellationToken);

        var completed = await Task.WhenAny(processorTask, delayTask).ConfigureAwait(false);
        if (completed == processorTask)
        {
            await processorTask.ConfigureAwait(false);
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        timeoutCts?.Cancel();
        throw new KafkaBatchProcessingTimeoutException(WorkerId, batch.Count, timeout);
    }

    private double CurrentUtilization() => _maxInFlightBatches == 0 ? 0 : (double)_pendingBatches.Count / _maxInFlightBatches;

    private List<TopicPartition> GetAssignedPartitions()
    {
        lock (_assignmentLock)
        {
            return _assignedPartitions.Count == 0 ? new List<TopicPartition>() : new List<TopicPartition>(_assignedPartitions);
        }
    }

    private sealed class PendingBatch
    {
        public PendingBatch(KafkaRecordBatch batch, Task task, DateTimeOffset startedAt)
        {
            Batch = batch;
            Task = task;
            StartedAt = startedAt;
        }

        public KafkaRecordBatch Batch { get; }
        public Task Task { get; }
        public DateTimeOffset StartedAt { get; }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None);

        switch (_processor)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    internal sealed class TestAccessor
    {
        private readonly KafkaConsumerWorker _owner;

        internal TestAccessor(KafkaConsumerWorker owner)
        {
            _owner = owner;
        }

        internal IEnumerable<TopicPartitionOffset> HandlePartitionsAssigned(
            IConsumer<byte[], byte[]> consumer,
            List<TopicPartition> partitions) => _owner.HandlePartitionsAssigned(consumer, partitions);

        internal void NotifyPartitionsRevoked(IReadOnlyList<TopicPartition> partitions) => _owner.NotifyPartitionsRevoked(partitions);

        internal IEnumerable<TopicPartitionOffset> ResolveRelativeAssignments(
            IConsumer<byte[], byte[]> consumer,
            List<TopicPartition> partitions) => _owner.ResolveRelativeAssignments(consumer, partitions);

        internal void PauseConsumer(IConsumer<byte[], byte[]> consumer) => _owner.PauseConsumer(consumer);

        internal void ResumeConsumer(IConsumer<byte[], byte[]> consumer) => _owner.ResumeConsumer(consumer);

        internal void EvaluateBackpressure(IConsumer<byte[], byte[]> consumer) => _owner.EvaluateBackpressure(consumer);

        internal TimeSpan GetCurrentPollTimeout() => _owner.GetCurrentPollTimeout();

        internal void SetFatalKafkaError(Error error) => _owner._fatalKafkaError = error;

        internal void ThrowIfFatalKafkaError() => _owner.ThrowIfFatalKafkaError();

        internal Task ProcessBatchWithTimeoutAsync(KafkaRecordBatch batch, CancellationToken cancellationToken) =>
            _owner.ProcessBatchWithTimeoutAsync(batch, cancellationToken);

        internal void SetAssignedPartitions(IEnumerable<TopicPartition> partitions)
        {
            lock (_owner._assignmentLock)
            {
                _owner._assignedPartitions = partitions.ToList();
            }
        }

        internal void SetPauseRequested(bool value) => _owner._pauseRequested = value;

        internal void SetPausedState(bool isPaused) => _owner._isPaused = isPaused;

        internal bool IsPaused => _owner._isPaused;

        internal bool PauseRequested => _owner._pauseRequested;

        internal void AddPendingBatch(KafkaRecordBatch batch)
        {
            _owner._pendingBatches.Add(new PendingBatch(batch, Task.CompletedTask, _owner._clock.Now));
            _owner._runtimeRecord.RecordBacklog(_owner._pendingBatches.Count, _owner._maxInFlightBatches);
        }

        internal int PendingBatchCount => _owner._pendingBatches.Count;

        internal ConsumerWorkerState CurrentState => _owner._runtimeRecord.Snapshot.State;
    }
}
