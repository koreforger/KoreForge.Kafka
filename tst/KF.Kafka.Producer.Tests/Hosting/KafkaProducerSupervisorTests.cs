using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Alerts;
using KF.Kafka.Producer.Backlog;
using KF.Kafka.Producer.Backpressure;
using KF.Kafka.Producer.Hosting;
using KF.Kafka.Producer.Metrics;
using KF.Kafka.Producer.Restart;
using KF.Kafka.Producer.Runtime;
using KF.Kafka.Producer.Serialization;
using KF.Kafka.Producer.Tests.Support;
using KF.Time;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace KF.Kafka.Producer.Tests.Hosting;

public sealed class KafkaProducerSupervisorTests
{
    [Fact]
    public async Task StartAndStopAsync_RestoresBacklog_PersistsOnStop_AndRunsAlerts()
    {
        var runtimeConfig = CreateRuntimeConfig(workerCount: 1, evaluationInterval: TimeSpan.FromMilliseconds(10));
        var serializerRegistry = new ProducerSerializerRegistry();
        serializerRegistry.RegisterSerializer(new PassThroughSerializer());

        var backlog = new TestBacklogStrategy();
        backlog.SeedRestore(new OutgoingEnvelope("restored", typeof(string), "topic-a", null, null, DateTimeOffset.UtcNow));

        var alerts = new ProducerAlertRegistry();
        var alertHits = 0;
        alerts.RegisterAlert(_ => true, _ => Interlocked.Increment(ref alertHits));

        var loggerFactory = new RecordingLoggerFactory();
        var createdProducers = new ConcurrentBag<IProducer<byte[], byte[]>>();
        Func<ProducerConfig, IProducer<byte[], byte[]>> producerFactory = _ =>
        {
            var producer = Substitute.For<IProducer<byte[], byte[]>>();
            producer.ProduceAsync(Arg.Any<string>(), Arg.Any<Message<byte[], byte[]>>(), Arg.Any<CancellationToken>())
                    .Returns(ci =>
                    {
                        var topic = ci.Arg<string>();
                        var message = ci.Arg<Message<byte[], byte[]>>();
                        return Task.FromResult(new DeliveryResult<byte[], byte[]>
                        {
                            Topic = topic,
                            Message = message,
                            Status = PersistenceStatus.Persisted
                        });
                    });
            producer.Flush(Arg.Any<TimeSpan>()).Returns(0);
            createdProducers.Add(producer);
            return producer;
        };

        var supervisor = new KafkaProducerSupervisor(
            runtimeConfig,
            serializerRegistry,
            new DefaultProducerBackpressurePolicy(loggerFactory.CreateLogger<DefaultProducerBackpressurePolicy>()),
            new DefaultProducerRestartPolicy(runtimeConfig.Extended.Restart),
            backlog,
            new ProducerMetricsBridge(null),
            new TestSystemClock(),
            alerts,
            loggerFactory,
            producerFactory);

        await supervisor.StartAsync(CancellationToken.None);
        await supervisor.Buffer.EnqueueAsync("topic-a", "payload");
        await WaitUntilAsync(() => supervisor.RuntimeState.Snapshot()[0].TotalProducedCount >= 2, TimeSpan.FromSeconds(2));
        await supervisor.StopAsync(CancellationToken.None);

        backlog.RestoreCalls.Should().Be(1);
        backlog.PersistCalls.Should().Be(1);
        alertHits.Should().BeGreaterThan(0);
        createdProducers.Should().HaveCount(1);
    }

    [Fact]
    public async Task WorkerFailure_TripsCircuitBreaker_WhenRestartPolicyRequestsIt()
    {
        var runtimeConfig = CreateRuntimeConfig(workerCount: 1);
        var serializerRegistry = Substitute.For<IProducerSerializerRegistry>();
        serializerRegistry.Serialize(Arg.Any<OutgoingEnvelope>()).Returns(_ => throw new InvalidOperationException("boom"));

        var restartPolicy = Substitute.For<IProducerRestartPolicy>();
        restartPolicy.Evaluate(Arg.Any<ProducerFailureContext>()).Returns(ProducerRestartDecision.TripCircuitBreaker());

        var supervisor = new KafkaProducerSupervisor(
            runtimeConfig,
            serializerRegistry,
            new DefaultProducerBackpressurePolicy(new RecordingLoggerFactory().CreateLogger<DefaultProducerBackpressurePolicy>()),
            restartPolicy,
            new TestBacklogStrategy(),
            new ProducerMetricsBridge(null),
            new TestSystemClock(),
            new ProducerAlertRegistry(),
            new RecordingLoggerFactory(),
            _ => CreateProducerStub());

        await supervisor.StartAsync(CancellationToken.None);
        await supervisor.Buffer.EnqueueAsync("topic-a", "payload");
        await WaitUntilAsync(() => restartPolicy.ReceivedCalls().Any(), TimeSpan.FromSeconds(2));
        await supervisor.StopAsync(CancellationToken.None);

        restartPolicy.Received().Evaluate(Arg.Any<ProducerFailureContext>());
        var snapshot = InvokeSnapshot(supervisor);
        snapshot.CircuitBreakerTripped.Should().BeTrue();
        snapshot.RuntimeRecords.Should().NotBeEmpty();
    }

    private static KafkaProducerRuntimeConfig CreateRuntimeConfig(int workerCount, TimeSpan? evaluationInterval = null)
    {
        var defaults = new ProducerTopicDefaults
        {
            WorkerCount = workerCount,
            ChannelCapacity = 8,
            BackpressureMode = ProducerBackpressureMode.Block
        };

        var extended = new ExtendedProducerSettings
        {
            TopicDefaults = defaults,
            Backlog = new ProducerBacklogSettings
            {
                PersistenceEnabled = true,
                ReplayEnabled = true,
                Directory = "backlog"
            },
            Alerts = new ProducerAlertSettings
            {
                Enabled = true,
                EvaluationInterval = evaluationInterval ?? TimeSpan.FromMilliseconds(50)
            }
        };

        return new KafkaProducerRuntimeConfig(
            new ProducerConfig { BootstrapServers = "localhost:9092", ClientId = "client" },
            extended,
            new List<string> { "topic-a" });
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var start = DateTime.UtcNow;
        while (!condition())
        {
            if (DateTime.UtcNow - start > timeout)
            {
                throw new TimeoutException("Condition was not satisfied within the allotted time.");
            }

            await Task.Delay(10);
        }
    }

    private static ProducerStatusSnapshot InvokeSnapshot(KafkaProducerSupervisor supervisor)
    {
        var method = typeof(KafkaProducerSupervisor).
            GetMethod("BuildSnapshot", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (ProducerStatusSnapshot)method.Invoke(supervisor, null)!;
    }

    private static IProducer<byte[], byte[]> CreateProducerStub()
    {
        var producer = Substitute.For<IProducer<byte[], byte[]>>();
        producer.Flush(Arg.Any<TimeSpan>()).Returns(0);
        return producer;
    }

    private sealed class PassThroughSerializer : IProducerSerializer<string>
    {
        public SerializedRecord Serialize(string message, OutgoingEnvelope envelope)
        {
            return new SerializedRecord
            {
                Topic = envelope.Topic,
                ValueBytes = System.Text.Encoding.UTF8.GetBytes(message),
                KeyBytes = envelope.Key is null ? null : System.Text.Encoding.UTF8.GetBytes(envelope.Key)
            };
        }
    }

    private sealed class TestBacklogStrategy : IProducerBacklogPersistenceStrategy
    {
        private readonly ConcurrentQueue<IReadOnlyList<OutgoingEnvelope>> _restoreQueue = new();

        public int PersistCalls { get; private set; }
        public int RestoreCalls { get; private set; }

        public void SeedRestore(params OutgoingEnvelope[] envelopes)
        {
            _restoreQueue.Enqueue(envelopes);
        }

        public Task PersistAsync(ProducerBacklogPersistContext context, CancellationToken cancellationToken)
        {
            PersistCalls++;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutgoingEnvelope>> RestoreAsync(ProducerBacklogRestoreContext context, CancellationToken cancellationToken)
        {
            RestoreCalls++;
            return Task.FromResult(_restoreQueue.TryDequeue(out var envelopes)
                ? envelopes
                : (IReadOnlyList<OutgoingEnvelope>)Array.Empty<OutgoingEnvelope>());
        }
    }

    private sealed class TestSystemClock : ISystemClock
    {
        public DateTimeOffset Now => DateTimeOffset.Now;
        public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
        public long TimestampTicks => Environment.TickCount64;
    }
}
