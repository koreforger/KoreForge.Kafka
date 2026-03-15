using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Consumer.Abstractions;
using KF.Kafka.Consumer.Backpressure;
using KF.Kafka.Consumer.Batch;
using KF.Kafka.Consumer.Execution;
using KF.Kafka.Consumer.Restart;
using KF.Kafka.Consumer.Runtime;
using KF.Kafka.Core.Runtime;
using KF.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Execution;

public sealed class KafkaConsumerSupervisorTests
{
    [Fact]
    public async Task StartAsync_StartsAllWorkers()
    {
        var workerFactory = new TestWorkerFactory();
        await using var supervisor = CreateSupervisor(workerCount: 2, workerFactory: workerFactory);

        await supervisor.StartAsync(CancellationToken.None);

        workerFactory.CreatedWorkers.Should().HaveCount(2);
        workerFactory.CreatedWorkers.Should().AllSatisfy(worker => worker.StartCalls.Should().Be(1));

        await supervisor.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StopAsync_StopsAndDisposesWorkers()
    {
        var workerFactory = new TestWorkerFactory();
        await using var supervisor = CreateSupervisor(workerCount: 1, workerFactory: workerFactory);
        await supervisor.StartAsync(CancellationToken.None);

        await supervisor.StopAsync(CancellationToken.None);

        var worker = workerFactory.CreatedWorkers[0];
        worker.StopCalls.Should().BeGreaterThan(0);
        worker.DisposeCalls.Should().Be(0); // disposed via IAsyncDisposable afterwards
    }

    [Fact]
    public async Task WorkerFailure_RestartsWhenPolicyAllows()
    {
        var restartPolicy = new RecordingRestartPolicy(RestartDecision.Restart(TimeSpan.Zero));
        var workerFactory = new TestWorkerFactory();
        await using var supervisor = CreateSupervisor(workerCount: 1, restartPolicy: restartPolicy, workerFactory: workerFactory);
        await supervisor.StartAsync(CancellationToken.None);

        var firstWorker = workerFactory.CreatedWorkers[0];
        firstWorker.Fail(new InvalidOperationException("boom"));

        SpinWait.SpinUntil(() => workerFactory.CreatedWorkers.Count >= 2, TimeSpan.FromSeconds(1)).Should().BeTrue();
        var replacement = workerFactory.CreatedWorkers[1];
        replacement.StartCalls.Should().Be(1);
        firstWorker.DisposeCalls.Should().BeGreaterThan(0);
        restartPolicy.Contexts.Should().HaveCount(1);

        await supervisor.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WorkerFailure_GivesUpWhenPolicyDenies()
    {
        var restartPolicy = new RecordingRestartPolicy(RestartDecision.GiveUp);
        var workerFactory = new TestWorkerFactory();
        await using var supervisor = CreateSupervisor(workerCount: 1, restartPolicy: restartPolicy, workerFactory: workerFactory);
        await supervisor.StartAsync(CancellationToken.None);

        var worker = workerFactory.CreatedWorkers[0];
        worker.Fail(new InvalidOperationException("fatal"));

        await Task.Delay(100);
        workerFactory.CreatedWorkers.Should().HaveCount(1);

        await supervisor.StopAsync(CancellationToken.None);
    }

    private static KafkaConsumerSupervisor CreateSupervisor(
        int workerCount,
        IRestartPolicy? restartPolicy = null,
        TestWorkerFactory? workerFactory = null)
    {
        var settings = new ExtendedConsumerSettings
        {
            ConsumerCount = workerCount,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 100,
            StartMode = StartMode.Earliest
        };

        var runtime = new KafkaConsumerRuntimeConfig(
            new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "group" },
            settings,
            new[] { "topic-1" },
            new KafkaSecuritySettings());

        var runtimeState = new KafkaConsumerRuntimeState(workerCount);
        var clock = new VirtualSystemClock(DateTimeOffset.UtcNow);
        return new KafkaConsumerSupervisor(
            runtime,
            () => new NoOpProcessor(),
            runtimeState,
            NullLoggerFactory.Instance,
            clock,
            new ThresholdBackpressurePolicy(),
            restartPolicy ?? new FixedRetryRestartPolicy(1, TimeSpan.Zero),
            workerFactory is null ? null : workerFactory.Create);
    }

    private sealed class NoOpProcessor : IKafkaBatchProcessor
    {
        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class TestWorkerFactory
    {
        public List<TestWorker> CreatedWorkers { get; } = new();

        public IKafkaConsumerWorker Create(KafkaConsumerWorkerCreationContext context)
        {
            var worker = new TestWorker(context);
            CreatedWorkers.Add(worker);
            return worker;
        }
    }

    private sealed class TestWorker : IKafkaConsumerWorker
    {
        private TaskCompletionSource<object?> _executionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly long _instanceId;
        private readonly Action<long> _resetFailures;

        public TestWorker(KafkaConsumerWorkerCreationContext context)
        {
            WorkerId = context.WorkerIndex + 1;
            _resetFailures = context.ResetFailures;
            _instanceId = context.BeginNewWorkerInstance();
        }

        public int WorkerId { get; }
        public int StartCalls { get; private set; }
        public int StopCalls { get; private set; }
        public int DisposeCalls { get; private set; }
        public Task Execution => _executionSource.Task;

        public Task StartAsync(CancellationToken supervisorToken)
        {
            StartCalls++;
            _resetFailures(_instanceId);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            StopCalls++;
            _executionSource.TrySetCanceled(cancellationToken);
            return Task.CompletedTask;
        }

        public void Fail(Exception exception)
        {
            _executionSource.TrySetException(exception);
        }

        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            _executionSource.TrySetCanceled();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingRestartPolicy : IRestartPolicy
    {
        private readonly Queue<RestartDecision> _decisions;

        public RecordingRestartPolicy(params RestartDecision[] decisions)
        {
            _decisions = new Queue<RestartDecision>(decisions);
        }

        public List<RestartContext> Contexts { get; } = new();

        public RestartDecision Evaluate(RestartContext context)
        {
            Contexts.Add(context);
            return _decisions.Count > 0 ? _decisions.Dequeue() : RestartDecision.GiveUp;
        }
    }
}
