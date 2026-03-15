using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Consumer.Abstractions;
using KF.Kafka.Consumer.Backpressure;
using KF.Kafka.Consumer.Batch;
using KF.Kafka.Consumer.Execution;
using KF.Kafka.Consumer.Tests.Support;
using KF.Kafka.Core.Runtime;
using KF.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Execution;

public sealed class KafkaConsumerWorkerTests
{
    [Fact]
    public void HandlePartitionsAssigned_ActivatesWorkerWithLatestOffsets()
    {
        var worker = CreateWorker(new ExtendedConsumerSettings
        {
            StartMode = StartMode.Latest
        });
        var accessor = worker.CreateTestAccessor();
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        var partitions = CreatePartitions();

        var offsets = accessor.HandlePartitionsAssigned(consumer.Object, partitions).ToList();

        offsets.Should().HaveCount(2);
        offsets.Should().AllSatisfy(offset => offset.Offset.Should().Be(Offset.End));
        accessor.CurrentState.Should().Be(ConsumerWorkerState.Active);
        consumer.Verify(c => c.Pause(It.IsAny<IEnumerable<TopicPartition>>()), Times.Never);
    }

    [Fact]
    public void HandlePartitionsAssigned_PausesWhenRequested()
    {
        var worker = CreateWorker(new ExtendedConsumerSettings
        {
            StartMode = StartMode.Earliest
        });
        var accessor = worker.CreateTestAccessor();
        accessor.SetPauseRequested(true);
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        var partitions = CreatePartitions();

        var offsets = accessor.HandlePartitionsAssigned(consumer.Object, partitions).ToList();

        offsets.Should().HaveCount(partitions.Count);
        consumer.Verify(c => c.Pause(It.Is<IEnumerable<TopicPartition>>(p => p.SequenceEqual(partitions))), Times.Once);
        accessor.IsPaused.Should().BeTrue();
        accessor.PauseRequested.Should().BeFalse();
        accessor.CurrentState.Should().Be(ConsumerWorkerState.Paused);
    }

    [Fact]
    public void ResolveRelativeAssignments_ReturnsOffsetsOrBeginning()
    {
        var worker = CreateWorker(new ExtendedConsumerSettings
        {
            StartMode = StartMode.RelativeTime,
            StartRelative = TimeSpan.FromMinutes(5)
        });
        var accessor = worker.CreateTestAccessor();
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        var partitions = CreatePartitions();

        consumer.Setup(c => c.OffsetsForTimes(It.IsAny<IEnumerable<TopicPartitionTimestamp>>(), It.IsAny<TimeSpan>()))
            .Returns<IEnumerable<TopicPartitionTimestamp>, TimeSpan>((requests, _) =>
            {
                var list = requests.ToList();
                return new List<TopicPartitionOffset>
                {
                    new(list[0].TopicPartition, new Offset(42)),
                    new TopicPartitionOffset(list[1].TopicPartition, Offset.Unset)
                };
            });

        var offsets = accessor.ResolveRelativeAssignments(consumer.Object, partitions).ToList();

        offsets[0].Offset.Should().Be(new Offset(42));
        offsets[1].Offset.Should().Be(Offset.Beginning);
    }

    [Fact]
    public void ResolveRelativeAssignments_ThrowsWhenRelativeWindowMissing()
    {
        var worker = CreateWorker(new ExtendedConsumerSettings
        {
            StartMode = StartMode.RelativeTime,
            StartRelative = null
        });
        var accessor = worker.CreateTestAccessor();
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        var partitions = CreatePartitions();

        var act = () => accessor.ResolveRelativeAssignments(consumer.Object, partitions).ToList();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Relative start mode requires a positive StartRelative value.");
    }

    [Fact]
    public void PauseConsumerWithoutAssignments_DefersPauseRequest()
    {
        var worker = CreateWorker();
        var accessor = worker.CreateTestAccessor();
        var consumer = new Mock<IConsumer<byte[], byte[]>>();

        accessor.PauseConsumer(consumer.Object);

        consumer.Verify(c => c.Pause(It.IsAny<IEnumerable<TopicPartition>>()), Times.Never);
        accessor.PauseRequested.Should().BeTrue();
        accessor.IsPaused.Should().BeFalse();
    }

    [Fact]
    public void ResumeConsumerWithAssignments_ResumesAndMarksActive()
    {
        var worker = CreateWorker();
        var accessor = worker.CreateTestAccessor();
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        var partitions = CreatePartitions();
        accessor.SetAssignedPartitions(partitions);
        accessor.SetPausedState(true);

        accessor.ResumeConsumer(consumer.Object);

        consumer.Verify(c => c.Resume(It.Is<IEnumerable<TopicPartition>>(p => p.SequenceEqual(partitions))), Times.Once);
        accessor.IsPaused.Should().BeFalse();
        accessor.PauseRequested.Should().BeFalse();
        accessor.CurrentState.Should().Be(ConsumerWorkerState.Active);
    }

    [Fact]
    public void EvaluateBackpressure_HonorsPolicyDecisions()
    {
        var policy = new RecordingBackpressurePolicy(BackpressureDecision.Pause, BackpressureDecision.Resume);
        var worker = CreateWorker(policy: policy);
        var accessor = worker.CreateTestAccessor();
        accessor.SetAssignedPartitions(CreatePartitions());
        var consumer = new Mock<IConsumer<byte[], byte[]>>();
        accessor.AddPendingBatch(KafkaRecordBatchFactory.CreateBatch(1));

        accessor.EvaluateBackpressure(consumer.Object);
        accessor.EvaluateBackpressure(consumer.Object);

        consumer.Verify(c => c.Pause(It.IsAny<IEnumerable<TopicPartition>>()), Times.Once);
        consumer.Verify(c => c.Resume(It.IsAny<IEnumerable<TopicPartition>>()), Times.Once);
        policy.Contexts.Should().HaveCount(2);
        policy.Contexts[0].BacklogSize.Should().Be(1);
    }

    private static List<TopicPartition> CreatePartitions() => new()
    {
        new TopicPartition("topic", new Partition(0)),
        new TopicPartition("topic", new Partition(1))
    };

    private static KafkaConsumerWorker CreateWorker(
        ExtendedConsumerSettings? settingsOverride = null,
        IBackpressurePolicy? policy = null)
    {
        var settings = settingsOverride ?? new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 100,
            StartMode = StartMode.Earliest
        };

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "tests"
        };

        var runtimeRecord = new ConsumerRuntimeRecord();
        var clock = new VirtualSystemClock(DateTimeOffset.UtcNow);
        return new KafkaConsumerWorker(
            workerIndex: 0,
            topics: new[] { "topic" },
            config: config,
            settings: settings,
            processor: new NoOpProcessor(),
            runtimeRecord: runtimeRecord,
            logger: NullLogger<KafkaConsumerWorker>.Instance,
            clock: clock,
            backpressurePolicy: policy ?? new ThresholdBackpressurePolicy(0.8, 0.4));
    }

    private sealed class NoOpProcessor : IKafkaBatchProcessor
    {
        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class RecordingBackpressurePolicy : IBackpressurePolicy
    {
        private readonly Queue<BackpressureDecision> _decisions;

        public RecordingBackpressurePolicy(params BackpressureDecision[] decisions)
        {
            _decisions = new Queue<BackpressureDecision>(decisions);
        }

        public List<BackpressureContext> Contexts { get; } = new();

        public BackpressureDecision Evaluate(BackpressureContext context)
        {
            Contexts.Add(context);
            return _decisions.Count > 0 ? _decisions.Dequeue() : BackpressureDecision.None;
        }
    }
}
