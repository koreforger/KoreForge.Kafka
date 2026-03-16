using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Generation;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Security;
using KF.Kafka.Configuration.Validation;
using KF.Kafka.Consumer.Abstractions;
using KF.Kafka.Consumer.Backpressure;
using KF.Kafka.Consumer.Batch;
using KF.Kafka.Consumer.Hosting;
using KF.Kafka.Consumer.IntegrationTests.Infrastructure;
using KF.Kafka.Consumer.Logging;
using KF.Kafka.Consumer.Pipelines;
using KF.Kafka.Consumer.Restart;
using KF.Kafka.Consumer.Runtime;
using KF.Kafka.Core.Runtime;
using KoreForge.Processing.Pipelines;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace KF.Kafka.Consumer.IntegrationTests;

[Collection(KafkaClusterCollection.CollectionName)]
public sealed class KafkaConsumerHostIntegrationTests
{
    private readonly KafkaTestClusterFixture _fixture;

    public KafkaConsumerHostIntegrationTests(KafkaTestClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Host_consumes_batches_from_topic()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        await _fixture.ProduceAsync(topic, partition: 0, messageCount: 12);

        var context = new RecordingProcessorContext(expectedCount: 12);
        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 2,
            MaxBatchSize = 6,
            MaxBatchWaitMs = 250,
            StartMode = StartMode.Earliest
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        context.GetValues().OrderBy(v => v).Should().Equal(Enumerable.Range(0, 12));
    }

    [Fact]
    public async Task Host_processes_batches_via_processing_pipeline()
    {
        var topic = $"consumer-pipeline-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var expectedCount = 6;
        await _fixture.ProduceAsync(topic, partition: 0, messageCount: expectedCount);

        var recorder = new ConsumerPipelineRecorder(expectedCount);

        Func<IProcessingPipeline<KafkaPipelineRecord, int>> pipelineFactory = () =>
            Pipeline
                .Start<KafkaPipelineRecord>()
                .UseStep(new PipelineValueCollectorStep(recorder))
                .Build();

        await using var host = BuildHost(
            topic,
            processorFactory: null,
            configureBuilder: builder => builder.UseProcessingPipeline<int>(pipelineFactory, pipelineBuilder =>
                pipelineBuilder.ConfigureExecutionOptions(new PipelineExecutionOptions { MaxDegreeOfParallelism = 2 })));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await recorder.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        recorder.GetValues().OrderBy(v => v).Should().Equal(Enumerable.Range(0, expectedCount));
    }

    [Fact]
    public async Task Host_respects_relative_start_offsets()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);

        await ProduceValuesAsync(topic, Enumerable.Range(0, 5), DateTime.UtcNow - TimeSpan.FromHours(2));
        await ProduceValuesAsync(topic, Enumerable.Range(5, 5), DateTime.UtcNow);

        var context = new RecordingProcessorContext(expectedCount: 5);
        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 200,
            StartMode = StartMode.RelativeTime,
            StartRelative = TimeSpan.FromMinutes(30)
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        context.GetValues().OrderBy(v => v).Should().Equal(Enumerable.Range(5, 5));
    }

    [Fact]
    public async Task Host_uses_security_profile_when_configured()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var context = new RecordingProcessorContext(expectedCount: 6);
        var securityProfileName = $"secure-{Guid.NewGuid():N}";
        var securitySettings = new KafkaSecuritySettings { Mode = KafkaSecurityMode.None };
        var recorder = new RecordingSecurityProvider();

        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
            {
                ConsumerCount = 1,
                MaxBatchSize = 3,
                MaxBatchWaitMs = 200
            },
            securityProfile: securityProfileName,
            securityProvider: recorder,
            configureRoot: root => root.SecurityProfiles[securityProfileName] = securitySettings);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await _fixture.ProduceAsync(topic, partition: 0, messageCount: 6);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        recorder.Invocations.Should().HaveCount(1);
        recorder.Invocations[0].Security.Should().BeSameAs(securitySettings);
    }

    [Fact]
    public async Task Host_rebalances_when_additional_consumer_joins_group()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 2);
        var sharedGroup = $"group-{Guid.NewGuid():N}";

        var partition0Values = Enumerable.Range(1_000, 10).ToArray();
        var partition1Values = Enumerable.Range(2_000, 10).ToArray();

        var contextA = new RecordingProcessorContext(partition0Values.Length);
        var contextB = new RecordingProcessorContext(partition1Values.Length);

        await using var hostA = BuildHost(
            topic,
            () => new RecordingProcessor(contextA),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 10,
            MaxBatchWaitMs = 200
        }, explicitGroupId: sharedGroup);

        await using var hostB = BuildHost(
            topic,
            () => new RecordingProcessor(contextB),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 10,
            MaxBatchWaitMs = 200
        }, explicitGroupId: sharedGroup);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await hostA.StartAsync(cts.Token);
        await hostB.StartAsync(cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
        await ProduceValuesAsync(topic, partition0Values, DateTime.UtcNow, partition: 0);
        await ProduceValuesAsync(topic, partition1Values, DateTime.UtcNow, partition: 1);

        await contextA.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await contextB.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);

        await hostB.StopAsync(cts.Token);
        await hostA.StopAsync(cts.Token);

        var valuesA = contextA.GetValues();
        var valuesB = contextB.GetValues();

        var aOwnsPartition0 = partition0Values.All(valuesA.Contains);
        var aOwnsPartition1 = partition1Values.All(valuesA.Contains);
        var bOwnsPartition0 = partition0Values.All(valuesB.Contains);
        var bOwnsPartition1 = partition1Values.All(valuesB.Contains);

        (aOwnsPartition0 ^ bOwnsPartition0).Should().BeTrue("partition 0 should belong to a single host");
        (aOwnsPartition1 ^ bOwnsPartition1).Should().BeTrue("partition 1 should belong to a single host");
        (aOwnsPartition0 || aOwnsPartition1).Should().BeTrue("host A should own one partition after rebalancing");
        (bOwnsPartition0 || bOwnsPartition1).Should().BeTrue("host B should own one partition after rebalancing");
    }

    [Fact]
    public async Task Host_recovers_partitions_when_member_leaves_group()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 2);
        var sharedGroup = $"group-{Guid.NewGuid():N}";

        var initialValues = Enumerable.Range(0, 5).ToArray();
        var recoveryPart0 = Enumerable.Range(2_000, 5).ToArray();
        var recoveryPart1 = Enumerable.Range(4_000, 5).ToArray();

        var contextA = new RecordingProcessorContext(initialValues.Length);
        var contextB = new RecordingProcessorContext(recoveryPart0.Length + recoveryPart1.Length, value => value >= 2_000);

        await using var hostA = BuildHost(
            topic,
            () => new RecordingProcessor(contextA),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 200
        }, explicitGroupId: sharedGroup);

        await using var hostB = BuildHost(
            topic,
            () => new RecordingProcessor(contextB),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 200
        }, explicitGroupId: sharedGroup);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await hostA.StartAsync(cts.Token);
        await ProduceValuesAsync(topic, initialValues, DateTime.UtcNow, partition: 0);
        await contextA.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);

        // Let the initial two-member rebalance settle before removing a member.
        await hostB.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);

        // StopAsync completes after consumer.Close() sends LeaveGroup.
        await hostA.StopAsync(cts.Token);

        // Produce recovery values immediately — they will sit in the topic
        // until the coordinator rebalances partition 0 back to hostB.
        await ProduceValuesAsync(topic, recoveryPart0, DateTime.UtcNow, partition: 0);
        await ProduceValuesAsync(topic, recoveryPart1, DateTime.UtcNow, partition: 1);
        await contextB.WaitAsync(TimeSpan.FromSeconds(60), cts.Token);

        await hostB.StopAsync(cts.Token);

        var valuesB = contextB.GetValues();
        recoveryPart0.All(valuesB.Contains).Should().BeTrue("partition 0 values should be reassigned after failure");
        recoveryPart1.All(valuesB.Contains).Should().BeTrue("partition 1 values should be reassigned after failure");
        contextB.GetPartitions().Should().Contain(new[] { 0, 1 });
    }

    [Fact]
    public async Task Host_runtime_state_tracks_consumption_metrics()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var context = new RecordingProcessorContext(expectedCount: 8);

        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 4,
            MaxBatchWaitMs = 200
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await _fixture.ProduceAsync(topic, partition: 0, messageCount: 8);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        var snapshot = host.RuntimeState.CreateSnapshot();
        snapshot.Workers.Should().HaveCount(1);
        snapshot.TotalConsumed.Should().BeGreaterOrEqualTo(8);
        snapshot.Workers.Single().TotalConsumedCount.Should().BeGreaterOrEqualTo(8);
        snapshot.Workers.Single().LastBatchSize.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Host_batches_respect_max_batch_size()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var context = new RecordingProcessorContext(expectedCount: 25);

        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 10,
            MaxBatchWaitMs = 1_000,
            StartMode = StartMode.Earliest
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await _fixture.ProduceAsync(topic, partition: 0, messageCount: 25);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        context.GetValues().OrderBy(v => v).Should().Equal(Enumerable.Range(0, 25));
        context.GetBatchSizes().Should().Equal(new[] { 10, 10, 5 });
    }

    [Fact]
    public async Task Host_flushes_partial_batches_on_window_expiry()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var context = new RecordingProcessorContext(expectedCount: 5);

        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 100,
            MaxBatchWaitMs = 200,
            StartMode = StartMode.Earliest
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        await host.StartAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(1), cts.Token);

        for (var i = 0; i < 5; i++)
        {
            await ProduceValueAsync(topic, i);
            if (i < 4)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(400), cts.Token);
            }
        }

        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        var batchSizes = context.GetBatchSizes();
        batchSizes.Should().HaveCount(5);
        batchSizes.Should().OnlyContain(size => size == 1);
    }

    [Fact]
    public async Task Host_processes_single_message_in_batch()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var context = new RecordingProcessorContext(expectedCount: 1);

        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 50,
            MaxBatchWaitMs = 500,
            StartMode = StartMode.Earliest
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await host.StartAsync(cts.Token);
        await ProduceValueAsync(topic, 42);
        await context.WaitAsync(TimeSpan.FromSeconds(10), cts.Token);
        await host.StopAsync(cts.Token);

        context.GetValues().Should().Equal(new[] { 42 });
        context.GetBatchSizes().Should().Equal(new[] { 1 });
    }

    [Fact]
    public async Task Multiple_workers_consume_without_duplicates()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 6);
        var expected = new List<int>();
        var context = new RecordingProcessorContext(expectedCount: 60);

        await using var host = BuildHost(
            topic,
            () => new RecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 3,
            MaxBatchSize = 20,
            MaxBatchWaitMs = 250,
            StartMode = StartMode.Earliest
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        await host.StartAsync(cts.Token);

        for (var partition = 0; partition < 6; partition++)
        {
            var values = Enumerable.Range(0, 10).Select(i => partition * 1_000 + i).ToArray();
            expected.AddRange(values);
            await ProduceValuesAsync(topic, values, DateTime.UtcNow, partition);
        }

        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        ConsumersStatusSnapshot snapshot = host.RuntimeState.CreateSnapshot();
        using (var waitCts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
        {
            while (snapshot.Workers.Any(worker => worker.TotalConsumedCount == 0))
            {
                await Task.Delay(100, waitCts.Token);
                snapshot = host.RuntimeState.CreateSnapshot();
            }
        }
        await host.StopAsync(cts.Token);

        var processed = context.GetValues();
        processed.Should().HaveCount(expected.Count);
        processed.Distinct().Should().HaveCount(expected.Count);
        processed.OrderBy(v => v).Should().Equal(expected.OrderBy(v => v));

        snapshot.Workers.Should().HaveCount(3);
        snapshot.Workers.Should().OnlyContain(worker => worker.TotalConsumedCount > 0);
    }

    [Fact]
    public async Task Backpressure_policy_pauses_and_resumes_IT30()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var messageCount = 60;
        var context = new RecordingProcessorContext(expectedCount: messageCount);
        var policy = new RecordingBackpressurePolicy(pauseThreshold: 4, resumeThreshold: 1);

        await using var host = BuildHost(
            topic,
            () => new SlowRecordingProcessor(context, TimeSpan.FromMilliseconds(250)),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 200,
            MaxInFlightBatches = 4,
            StartMode = StartMode.Earliest
        },
            backpressurePolicy: policy);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await host.StartAsync(cts.Token);

        await _fixture.ProduceAsync(topic, partition: 0, messageCount: messageCount);
        await context.WaitAsync(TimeSpan.FromSeconds(90), cts.Token);
        await host.StopAsync(cts.Token);

        var decisions = policy.GetDecisions();
        decisions.Should().Contain(BackpressureDecision.Pause);
        decisions.Should().Contain(BackpressureDecision.Resume);
    }

    [Fact]
    public async Task Backpressure_state_reflected_in_snapshot_IT31()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var messageCount = 60;
        var context = new RecordingProcessorContext(expectedCount: messageCount);
        var policy = new RecordingBackpressurePolicy(pauseThreshold: 4, resumeThreshold: 1);

        await using var host = BuildHost(
            topic,
            () => new SlowRecordingProcessor(context, TimeSpan.FromMilliseconds(250)),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 200,
            MaxInFlightBatches = 4,
            StartMode = StartMode.Earliest
        },
            backpressurePolicy: policy);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await host.StartAsync(cts.Token);

        using var monitorCts = new CancellationTokenSource();
        var snapshots = new ConcurrentQueue<ConsumersStatusSnapshot>();
        var monitorTask = Task.Run(async () =>
        {
            try
            {
                while (!monitorCts.IsCancellationRequested)
                {
                    snapshots.Enqueue(host.RuntimeState.CreateSnapshot());
                    await Task.Delay(TimeSpan.FromMilliseconds(100), monitorCts.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        await _fixture.ProduceAsync(topic, partition: 0, messageCount: messageCount);
        await context.WaitAsync(TimeSpan.FromSeconds(90), cts.Token);
        monitorCts.Cancel();
        await monitorTask;
        await host.StopAsync(cts.Token);

        snapshots.Should().NotBeEmpty();
        var snapshotList = snapshots.ToList();
        snapshotList.Should().Contain(snapshot => snapshot.Workers.Any(worker => worker.State == ConsumerWorkerState.Paused));
        var resumedSnapshot = snapshotList.LastOrDefault(snapshot => snapshot.Workers.All(worker => worker.State == ConsumerWorkerState.Active));
        resumedSnapshot.Should().NotBeNull();
        resumedSnapshot!.PercentagePausedConsumers.Should().Be(0);
    }

    [Fact]
    public async Task Coordinated_backpressure_pauses_all_workers_IT32()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic, partitions: 2);
        var messageCount = 80;
        var context = new RecordingProcessorContext(expectedCount: messageCount);
        var loggerFactory = new TestLoggerFactory();

        await using var host = BuildHost(
            topic,
            () => new SlowRecordingProcessor(context, TimeSpan.FromMilliseconds(300)),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 2,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 100,
            MaxInFlightBatches = 2,
            StartMode = StartMode.Earliest,
            Backpressure = new ConsumerBackpressureSettings
            {
                Coordinated = true,
                PauseUtilization = 0.9,
                ResumeUtilization = 0.45
            }
        },
            loggerFactory: loggerFactory);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(180));
        await host.StartAsync(cts.Token);

        await _fixture.ProduceAsync(topic, partition: 0, messageCount: messageCount);
        await _fixture.ProduceAsync(topic, partition: 1, messageCount: messageCount);

        await WaitForSnapshotAsync(
            host,
            snapshot => snapshot.PercentagePausedConsumers >= 100,
            TimeSpan.FromSeconds(60),
            cts.Token);

        await context.WaitAsync(TimeSpan.FromSeconds(120), cts.Token);
        await host.StopAsync(cts.Token);

        var logEntries = loggerFactory.GetEntries();
        var pauseEvents = logEntries.Count(entry => entry.EventId.Id == (int)ConsumerLogEvents.Worker_Backpressure_Paused);
        var resumeEvents = logEntries.Count(entry => entry.EventId.Id == (int)ConsumerLogEvents.Worker_Backpressure_Resumed);

        pauseEvents.Should().BeGreaterOrEqualTo(2);
        resumeEvents.Should().BeGreaterOrEqualTo(2);
    }

    [Fact]
    public async Task Worker_recovers_from_transient_failure_IT40()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var messageCount = 30;
        var context = new RecordingProcessorContext(expectedCount: messageCount);
        var restartPolicy = new RecordingRestartPolicy(maxRetries: 3, backoff: TimeSpan.FromMilliseconds(200));
        var loggerFactory = new TestLoggerFactory();
        var failurePlan = new TransientFailurePlan(failuresBeforeRecovery: 1);

        await using var host = BuildHost(
            topic,
            () => new FlakyRecordingProcessor(context, failurePlan),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 10,
            MaxBatchWaitMs = 200,
            StartMode = StartMode.Earliest
        },
            loggerFactory: loggerFactory,
            restartPolicy: restartPolicy);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await host.StartAsync(cts.Token);

        await ProduceValuesAsync(topic, Enumerable.Range(0, messageCount));
        await restartPolicy.WaitForFailureAsync(TimeSpan.FromSeconds(30), cts.Token);

        try
        {
            await WaitForSnapshotAsync(
                host,
                snapshot => snapshot.Workers.Any(worker => worker.State == ConsumerWorkerState.Failed),
                TimeSpan.FromSeconds(5),
                cts.Token);
        }
        catch (TimeoutException)
        {
            // Under coverage instrumentation the worker can restart before we observe the failed state.
            // The restart policy and log assertions below still guarantee that a failure occurred.
        }

        await context.WaitAsync(TimeSpan.FromSeconds(90), cts.Token);

        await WaitForSnapshotAsync(
            host,
            snapshot => snapshot.Workers.Any(worker => worker.State == ConsumerWorkerState.Active && worker.TotalConsumedCount >= messageCount),
            TimeSpan.FromSeconds(30),
            cts.Token);

        await host.StopAsync(cts.Token);

        restartPolicy.Invocations.Should().HaveCount(1);
        var logEntries = loggerFactory.GetEntries();
        logEntries.Should().Contain(entry => entry.EventId.Id == (int)ConsumerLogEvents.Worker_Restart_Scheduled);

        context.GetValues().Distinct().OrderBy(v => v).Should().Equal(Enumerable.Range(0, messageCount));
    }

    [Fact]
    public async Task Worker_stops_restarting_after_retry_limit_IT41()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var restartPolicy = new RecordingRestartPolicy(maxRetries: 2, backoff: TimeSpan.FromMilliseconds(50));
        var loggerFactory = new TestLoggerFactory();

        await using var host = BuildHost(
            topic,
            () => new AlwaysFailingProcessor(),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 200,
            StartMode = StartMode.Earliest
        },
            loggerFactory: loggerFactory,
            restartPolicy: restartPolicy);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await host.StartAsync(cts.Token);

        await ProduceValuesAsync(topic, Enumerable.Range(0, 10));
        await restartPolicy.WaitForGiveUpAsync(TimeSpan.FromSeconds(60), cts.Token);

        await WaitForSnapshotAsync(
            host,
            snapshot => snapshot.Workers.Any(worker => worker.State == ConsumerWorkerState.Failed),
            TimeSpan.FromSeconds(10),
            cts.Token);

        await host.StopAsync(cts.Token);

        restartPolicy.Invocations.Should().HaveCount(3);

        var logEntries = loggerFactory.GetEntries();
        logEntries.Should().Contain(entry => entry.EventId.Id == (int)ConsumerLogEvents.Worker_Restart_GiveUp);
    }

    [Fact]
    public async Task Processor_receives_raw_payloads_IT20()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var payloads = Enumerable.Range(0, 5)
            .Select(i => $"{{\"value\":{i}}}")
            .ToArray();
        var payloadBytes = payloads.Select(payload => Encoding.UTF8.GetBytes(payload)).ToArray();
        var context = new RawPayloadRecordingProcessorContext(expectedCount: payloads.Length);

        await using var host = BuildHost(
            topic,
            () => new RawPayloadRecordingProcessor(context),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = payloads.Length,
            MaxBatchWaitMs = 250,
            StartMode = StartMode.Earliest
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);
        await ProduceRawValuesAsync(topic, payloadBytes);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        context.GetPayloads().Should().Equal(payloads);
        context.GetDeserializedValues().Should().Equal(Enumerable.Range(0, payloads.Length));
    }

    [Fact]
    public async Task Poison_message_is_skipped_and_logged_IT21()
    {
        var topic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);
        var context = new PoisonSkippingProcessorContext(expectedValidCount: 4);
        var loggerFactory = new TestLoggerFactory();

        await using var host = BuildHost(
            topic,
            () => new PoisonSkippingProcessor(
                context,
                loggerFactory.CreateLogger(nameof(PoisonSkippingProcessor))),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 5,
            MaxBatchWaitMs = 250,
            StartMode = StartMode.Earliest
        },
            loggerFactory: loggerFactory);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await host.StartAsync(cts.Token);

        var malformed = Encoding.UTF8.GetBytes("{\"value\":\"oops\"");
        var validPayloads = Enumerable.Range(0, 4)
            .Select(i => Encoding.UTF8.GetBytes($"{{\"value\":{i}}}"))
            .ToList();
        var produced = new List<byte[]> { malformed };
        produced.AddRange(validPayloads);

        await ProduceRawValuesAsync(topic, produced);
        await context.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        context.GetValidValues().OrderBy(v => v).Should().Equal(Enumerable.Range(0, 4));
        context.GetPoisonPayloads().Should().ContainSingle();
        loggerFactory.GetEntries().Should().Contain(entry =>
            entry.Message.Contains("poison", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task Processing_modes_sequential_and_parallel_IT22()
    {
        var sequentialTopic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(sequentialTopic);
        var sequentialContext = new ProcessingModeTestContext(expectedCount: 10);

        await using (var sequentialHost = BuildHost(
            sequentialTopic,
            () => new ProcessingModeProcessor(TestProcessingMode.Sequential, sequentialContext),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 10,
            MaxBatchWaitMs = 200,
            StartMode = StartMode.Earliest
        }))
        {
            using var sequentialCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await sequentialHost.StartAsync(sequentialCts.Token);
            await ProduceValuesAsync(sequentialTopic, Enumerable.Range(0, 10));
            await sequentialContext.WaitAsync(TimeSpan.FromSeconds(30), sequentialCts.Token);
            await sequentialHost.StopAsync(sequentialCts.Token);
        }

        sequentialContext.GetValues().Should().Equal(Enumerable.Range(0, 10));
        sequentialContext.MaxConcurrency.Should().Be(1);

        var parallelTopic = $"consumer-int-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(parallelTopic);
        var parallelContext = new ProcessingModeTestContext(expectedCount: 20);

        await using (var parallelHost = BuildHost(
            parallelTopic,
            () => new ProcessingModeProcessor(TestProcessingMode.Parallel, parallelContext),
            new ExtendedConsumerSettings
        {
            ConsumerCount = 1,
            MaxBatchSize = 20,
            MaxBatchWaitMs = 200,
            StartMode = StartMode.Earliest
        }))
        {
            using var parallelCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await parallelHost.StartAsync(parallelCts.Token);
            await ProduceValuesAsync(parallelTopic, Enumerable.Range(0, 20));
            await parallelContext.WaitAsync(TimeSpan.FromSeconds(30), parallelCts.Token);
            await parallelHost.StopAsync(parallelCts.Token);
        }

        var parallelValues = parallelContext.GetValues();
        parallelValues.Should().HaveCount(20);
        parallelValues.Distinct().Should().HaveCount(20);
        parallelContext.MaxConcurrency.Should().BeGreaterThan(1);
    }

    private KafkaConsumerHost BuildHost(
        string topic,
        Func<IKafkaBatchProcessor>? processorFactory = null,
        ExtendedConsumerSettings? extendedSettings = null,
        string? explicitGroupId = null,
        string? securityProfile = null,
        IKafkaSecurityProvider? securityProvider = null,
        Action<KafkaConfigurationRootOptions>? configureRoot = null,
        ILoggerFactory? loggerFactory = null,
        IBackpressurePolicy? backpressurePolicy = null,
        IRestartPolicy? restartPolicy = null,
        Func<KafkaConsumerHostBuilder, KafkaConsumerHostBuilder>? configureBuilder = null)
    {
        if (processorFactory is null && configureBuilder is null)
        {
            throw new ArgumentException("A processor factory or builder configuration must be provided.", nameof(processorFactory));
        }

        var profileName = $"integration-{Guid.NewGuid():N}";
        var root = new KafkaConfigurationRootOptions();
        configureRoot?.Invoke(root);
        root.Clusters["primary"] = new KafkaClusterSettings
        {
            BootstrapServers = _fixture.BootstrapServers
        };

        var profile = new KafkaProfileSettings
        {
            Type = KafkaClientType.Consumer,
            Cluster = "primary",
            SecurityProfile = securityProfile,
            ExplicitGroupId = explicitGroupId ?? $"group-{Guid.NewGuid():N}",
            ExtendedConsumer = extendedSettings ?? new ExtendedConsumerSettings
            {
                ConsumerCount = 1,
                MaxBatchSize = 10,
                MaxBatchWaitMs = 500,
                StartMode = StartMode.Earliest
            }
        };

        profile.Topics.Add(topic);
        profile.ConfluentOptions["auto.offset.reset"] = "earliest";
        profile.ConfluentOptions["enable.auto.commit"] = "false";
        profile.ConfluentOptions["session.timeout.ms"] = "6000";
        profile.ConfluentOptions["heartbeat.interval.ms"] = "2000";
        root.Profiles[profileName] = profile;

        var factory = new KafkaClientConfigFactory(
            new StaticOptionsMonitor<KafkaConfigurationRootOptions>(root),
            new DefaultKafkaGroupIdGenerator(),
            securityProvider ?? new NoOpSecurityProvider(),
            new KafkaConfigValidator(),
            NullLogger<KafkaClientConfigFactory>.Instance);

        var builder = KafkaConsumerHost.Create()
            .UseKafkaConfigurationProfile(profileName, factory)
            .UseRestartPolicy(restartPolicy ?? new FixedRetryRestartPolicy())
            .UseLoggerFactory(loggerFactory ?? NullLoggerFactory.Instance);

        if (backpressurePolicy is not null)
        {
            builder = builder.UseBackpressurePolicy(backpressurePolicy);
        }

        builder = configureBuilder?.Invoke(builder) ?? builder.UseProcessor(processorFactory!);
        return builder.Build();
    }

    private async Task ProduceValuesAsync(string topic, IEnumerable<int> values, DateTime? timestampUtc = null, int partition = 0)
    {
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = _fixture.BootstrapServers,
            Acks = Acks.All
        }).Build();

        foreach (var value in values)
        {
            var message = new Message<byte[], byte[]>
            {
                Value = BitConverter.GetBytes(value),
                Timestamp = timestampUtc.HasValue ? new Timestamp(timestampUtc.Value) : Timestamp.Default
            };

            await producer.ProduceAsync(new TopicPartition(topic, partition), message);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private Task ProduceValueAsync(string topic, int value, DateTime? timestampUtc = null, int partition = 0) =>
        ProduceValuesAsync(topic, new[] { value }, timestampUtc, partition);

    private async Task ProduceRawValuesAsync(string topic, IEnumerable<byte[]> values, DateTime? timestampUtc = null, int partition = 0)
    {
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = _fixture.BootstrapServers,
            Acks = Acks.All
        }).Build();

        foreach (var value in values)
        {
            var message = new Message<byte[], byte[]>
            {
                Value = value,
                Timestamp = timestampUtc.HasValue ? new Timestamp(timestampUtc.Value) : Timestamp.Default
            };

            await producer.ProduceAsync(new TopicPartition(topic, partition), message);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private Task ProduceRawValueAsync(string topic, byte[] value, DateTime? timestampUtc = null, int partition = 0) =>
        ProduceRawValuesAsync(topic, new[] { value }, timestampUtc, partition);

    private sealed class RecordingProcessorContext
    {
        private readonly int _expectedCount;
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Func<int, bool>? _valueFilter;
        private int _matched;

        public RecordingProcessorContext(int expectedCount, Func<int, bool>? valueFilter = null)
        {
            _expectedCount = expectedCount;
            _valueFilter = valueFilter;
        }

        public ConcurrentQueue<int> Values { get; } = new();
        public ConcurrentQueue<int> Partitions { get; } = new();
        public ConcurrentQueue<int> BatchSizes { get; } = new();

        public void Record(KafkaRecordBatch batch)
        {
            BatchSizes.Enqueue(batch.Count);
            foreach (var record in batch.Records)
            {
                var value = BitConverter.ToInt32(record.Message.Value);
                Values.Enqueue(value);
                Partitions.Enqueue(record.Partition.Value);

                if (_valueFilter is null || _valueFilter(value))
                {
                    if (Interlocked.Increment(ref _matched) >= _expectedCount)
                    {
                        _completion.TrySetResult(true);
                    }
                }
            }
        }

        public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
            _completion.Task.WaitAsync(timeout, cancellationToken);

        public IReadOnlyCollection<int> GetValues() => Values.ToArray();
        public IReadOnlyCollection<int> GetPartitions() => Partitions.ToArray();
        public IReadOnlyCollection<int> GetBatchSizes() => BatchSizes.ToArray();
    }

    private sealed class ConsumerPipelineRecorder
    {
        private readonly int _expectedCount;
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _processed;

        public ConsumerPipelineRecorder(int expectedCount)
        {
            _expectedCount = expectedCount;
        }

        private ConcurrentQueue<int> Values { get; } = new();

        public void Record(int value)
        {
            Values.Enqueue(value);
            if (Interlocked.Increment(ref _processed) >= _expectedCount)
            {
                _completion.TrySetResult(true);
            }
        }

        public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
            _completion.Task.WaitAsync(timeout, cancellationToken);

        public IReadOnlyCollection<int> GetValues() => Values.ToArray();
    }

    private sealed class RecordingProcessor : IKafkaBatchProcessor
    {
        private readonly RecordingProcessorContext _context;

        public RecordingProcessor(RecordingProcessorContext context)
        {
            _context = context;
        }

        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            _context.Record(batch);
            return Task.CompletedTask;
        }
    }

    private sealed class PipelineValueCollectorStep : IPipelineStep<KafkaPipelineRecord, int>
    {
        private readonly ConsumerPipelineRecorder _recorder;

        public PipelineValueCollectorStep(ConsumerPipelineRecorder recorder)
        {
            _recorder = recorder;
        }

        public ValueTask<StepOutcome<int>> InvokeAsync(KafkaPipelineRecord input, PipelineContext context, CancellationToken cancellationToken)
        {
            var value = BitConverter.ToInt32(input.Value.Span);
            _recorder.Record(value);
            return ValueTask.FromResult(StepOutcome<int>.Continue(value));
        }
    }

    private sealed class SlowRecordingProcessor : IKafkaBatchProcessor
    {
        private readonly RecordingProcessorContext _context;
        private readonly TimeSpan _delay;

        public SlowRecordingProcessor(RecordingProcessorContext context, TimeSpan delay)
        {
            _context = context;
            _delay = delay;
        }

        public async Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            await Task.Delay(_delay, cancellationToken);
            _context.Record(batch);
        }
    }

    private sealed class TransientFailurePlan
    {
        private int _remainingFailures;

        public TransientFailurePlan(int failuresBeforeRecovery)
        {
            if (failuresBeforeRecovery < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(failuresBeforeRecovery));
            }

            _remainingFailures = failuresBeforeRecovery;
        }

        public bool ShouldFail()
        {
            if (_remainingFailures <= 0)
            {
                return false;
            }

            var remaining = Interlocked.Decrement(ref _remainingFailures);
            return remaining >= 0;
        }
    }

    private sealed class FlakyRecordingProcessor : IKafkaBatchProcessor
    {
        private readonly RecordingProcessorContext _context;
        private readonly TransientFailurePlan _plan;

        public FlakyRecordingProcessor(RecordingProcessorContext context, TransientFailurePlan plan)
        {
            _context = context;
            _plan = plan;
        }

        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            if (_plan.ShouldFail())
            {
                throw new InvalidOperationException("Simulated processor failure.");
            }

            _context.Record(batch);
            return Task.CompletedTask;
        }
    }

    private sealed class AlwaysFailingProcessor : IKafkaBatchProcessor
    {
        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Simulated permanent processor failure.");
        }
    }

    private sealed class RawPayloadRecordingProcessorContext
    {
        private readonly int _expectedCount;
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _recorded;

        public RawPayloadRecordingProcessorContext(int expectedCount)
        {
            _expectedCount = expectedCount;
        }

        public ConcurrentQueue<string> Payloads { get; } = new();
        public ConcurrentQueue<int> DeserializedValues { get; } = new();

        public void Record(byte[] payload)
        {
            var text = Encoding.UTF8.GetString(payload);
            Payloads.Enqueue(text);

            using var json = JsonDocument.Parse(payload);
            DeserializedValues.Enqueue(json.RootElement.GetProperty("value").GetInt32());

            if (Interlocked.Increment(ref _recorded) >= _expectedCount)
            {
                _completion.TrySetResult(true);
            }
        }

        public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
            _completion.Task.WaitAsync(timeout, cancellationToken);

        public IReadOnlyCollection<string> GetPayloads() => Payloads.ToArray();
        public IReadOnlyCollection<int> GetDeserializedValues() => DeserializedValues.ToArray();
    }

    private sealed class RawPayloadRecordingProcessor : IKafkaBatchProcessor
    {
        private readonly RawPayloadRecordingProcessorContext _context;

        public RawPayloadRecordingProcessor(RawPayloadRecordingProcessorContext context)
        {
            _context = context;
        }

        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            foreach (var record in batch.Records)
            {
                _context.Record(record.Message.Value);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class PoisonSkippingProcessorContext
    {
        private readonly int _expectedValidCount;
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _validCount;

        public PoisonSkippingProcessorContext(int expectedValidCount)
        {
            _expectedValidCount = expectedValidCount;
        }

        public ConcurrentQueue<int> ValidValues { get; } = new();
        public ConcurrentQueue<string> PoisonPayloads { get; } = new();

        public void RecordSuccess(int value)
        {
            ValidValues.Enqueue(value);
            if (Interlocked.Increment(ref _validCount) >= _expectedValidCount)
            {
                _completion.TrySetResult(true);
            }
        }

        public void RecordPoison(string payload)
        {
            PoisonPayloads.Enqueue(payload);
        }

        public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
            _completion.Task.WaitAsync(timeout, cancellationToken);

        public IReadOnlyCollection<int> GetValidValues() => ValidValues.ToArray();
        public IReadOnlyCollection<string> GetPoisonPayloads() => PoisonPayloads.ToArray();
    }

    private sealed class PoisonSkippingProcessor : IKafkaBatchProcessor
    {
        private readonly PoisonSkippingProcessorContext _context;
        private readonly ILogger _logger;

        public PoisonSkippingProcessor(PoisonSkippingProcessorContext context, ILogger logger)
        {
            _context = context;
            _logger = logger;
        }

        public Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            foreach (var record in batch.Records)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var payload = record.Message.Value;

                try
                {
                    using var json = JsonDocument.Parse(payload);
                    var value = json.RootElement.GetProperty("value").GetInt32();
                    _context.RecordSuccess(value);
                }
                catch (JsonException ex)
                {
                    var text = Encoding.UTF8.GetString(payload);
                    _logger.LogWarning(ex, "poison payload skipped: {Payload}", text);
                    _context.RecordPoison(text);
                }
            }

            return Task.CompletedTask;
        }
    }

    private enum TestProcessingMode
    {
        Sequential,
        Parallel
    }

    private sealed class ProcessingModeTestContext
    {
        private readonly int _expectedCount;
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _processed;
        private int _inFlight;
        private int _maxConcurrency;

        public ProcessingModeTestContext(int expectedCount)
        {
            _expectedCount = expectedCount;
        }

        public ConcurrentQueue<int> Values { get; } = new();

        public void OperationStarted()
        {
            var current = Interlocked.Increment(ref _inFlight);
            int snapshot;
            do
            {
                snapshot = Volatile.Read(ref _maxConcurrency);
                if (current <= snapshot)
                {
                    return;
                }
            }
            while (Interlocked.CompareExchange(ref _maxConcurrency, current, snapshot) != snapshot);
        }

        public void OperationCompleted()
        {
            Interlocked.Decrement(ref _inFlight);
        }

        public void Record(int value)
        {
            Values.Enqueue(value);
            if (Interlocked.Increment(ref _processed) >= _expectedCount)
            {
                _completion.TrySetResult(true);
            }
        }

        public Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
            _completion.Task.WaitAsync(timeout, cancellationToken);

        public IReadOnlyCollection<int> GetValues() => Values.ToArray();
        public int MaxConcurrency => Volatile.Read(ref _maxConcurrency);
    }

    private sealed class ProcessingModeProcessor : IKafkaBatchProcessor
    {
        private readonly TestProcessingMode _mode;
        private readonly ProcessingModeTestContext _context;

        public ProcessingModeProcessor(TestProcessingMode mode, ProcessingModeTestContext context)
        {
            _mode = mode;
            _context = context;
        }

        public async Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
        {
            if (_mode == TestProcessingMode.Sequential)
            {
                foreach (var record in batch.Records)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await ProcessRecordAsync(record, cancellationToken).ConfigureAwait(false);
                }
                return;
            }

            var tasks = batch.Records.Select(record => ProcessRecordAsync(record, cancellationToken));
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private async Task ProcessRecordAsync(ConsumeResult<byte[], byte[]> record, CancellationToken cancellationToken)
        {
            _context.OperationStarted();
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken).ConfigureAwait(false);
                var value = BitConverter.ToInt32(record.Message.Value);
                _context.Record(value);
            }
            finally
            {
                _context.OperationCompleted();
            }
        }
    }

    private sealed class TestLoggerFactory : ILoggerFactory
    {
        private readonly ConcurrentQueue<LogEntry> _entries = new();

        public ILogger CreateLogger(string categoryName) => new TestLogger(categoryName, _entries);

        public void AddProvider(ILoggerProvider provider)
        {
            // Providers not used in tests.
        }

        public IReadOnlyCollection<LogEntry> GetEntries() => _entries.ToArray();

        public void Dispose()
        {
        }

        private sealed class TestLogger : ILogger
        {
            private readonly string _categoryName;
            private readonly ConcurrentQueue<LogEntry> _entries;

            public TestLogger(string categoryName, ConcurrentQueue<LogEntry> entries)
            {
                _categoryName = categoryName;
                _entries = entries;
            }

            public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                var message = formatter?.Invoke(state, exception) ?? string.Empty;
                _entries.Enqueue(new LogEntry(_categoryName, logLevel, eventId, message, exception));
            }

            private sealed class NullScope : IDisposable
            {
                public static readonly NullScope Instance = new();

                public void Dispose()
                {
                }
            }
        }

        public sealed record LogEntry(string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);
    }

    private sealed class RecordingRestartPolicy : IRestartPolicy
    {
        private readonly int _maxRetries;
        private readonly TimeSpan _backoff;
        private readonly ConcurrentQueue<RestartContext> _invocations = new();
        private readonly TaskCompletionSource<bool> _giveUpSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _failureObservedSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public RecordingRestartPolicy(int maxRetries, TimeSpan backoff)
        {
            if (maxRetries < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxRetries));
            }

            _maxRetries = maxRetries;
            _backoff = backoff;
        }

        public IReadOnlyCollection<RestartContext> Invocations => _invocations.ToArray();

        public RestartDecision Evaluate(RestartContext context)
        {
            _invocations.Enqueue(context);
            _failureObservedSource.TrySetResult(true);

            if (context.ConsecutiveFailures <= _maxRetries)
            {
                return RestartDecision.Restart(_backoff);
            }

            _giveUpSource.TrySetResult(true);
            return RestartDecision.GiveUp;
        }

        public Task WaitForGiveUpAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return _giveUpSource.Task.WaitAsync(timeout, cancellationToken);
        }

        public Task WaitForFailureAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return _failureObservedSource.Task.WaitAsync(timeout, cancellationToken);
        }
    }

    private static async Task WaitForSnapshotAsync(
        KafkaConsumerHost host,
        Func<ConsumersStatusSnapshot, bool> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linkedCts.CancelAfter(timeout);
        ConsumersStatusSnapshot? lastSnapshot = null;

        try
        {
            while (true)
            {
                linkedCts.Token.ThrowIfCancellationRequested();
                var snapshot = host.RuntimeState.CreateSnapshot();
                lastSnapshot = snapshot;
                if (predicate(snapshot))
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(20), linkedCts.Token);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            var snapshotInfo = DescribeSnapshot(lastSnapshot);
            throw new TimeoutException($"Timed out waiting for consumer snapshot after {timeout}. Last snapshot: {snapshotInfo}");
        }
    }

    private static string DescribeSnapshot(ConsumersStatusSnapshot? snapshot)
    {
        if (snapshot is null || snapshot.Workers.Count == 0)
        {
            return "<no workers>";
        }

        var workerStates = snapshot.Workers
            .Select((worker, index) => $"W{index + 1}: state={worker.State}, consumed={worker.TotalConsumedCount}, backlog={worker.BacklogSize}/{worker.BacklogCapacity}")
            .ToArray();

        return string.Join(" | ", workerStates);
    }

    private sealed class RecordingBackpressurePolicy : IBackpressurePolicy
    {
        private readonly int _pauseThreshold;
        private readonly int _resumeThreshold;
        private readonly ConcurrentQueue<BackpressureDecision> _decisions = new();

        public RecordingBackpressurePolicy(int pauseThreshold, int resumeThreshold)
        {
            _pauseThreshold = pauseThreshold;
            _resumeThreshold = resumeThreshold;
        }

        public BackpressureDecision Evaluate(BackpressureContext context)
        {
            if (context.BacklogSize >= _pauseThreshold)
            {
                _decisions.Enqueue(BackpressureDecision.Pause);
                return BackpressureDecision.Pause;
            }

            if (context.BacklogSize <= _resumeThreshold)
            {
                _decisions.Enqueue(BackpressureDecision.Resume);
                return BackpressureDecision.Resume;
            }

            return BackpressureDecision.None;
        }

        public IReadOnlyCollection<BackpressureDecision> GetDecisions() => _decisions.ToArray();
    }

    private sealed class RecordingSecurityProvider : IKafkaSecurityProvider
    {
        public List<(string ProfileName, KafkaSecuritySettings Security)> Invocations { get; } = new();

        public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
        {
            Invocations.Add((profileName, security));
        }
    }

    private sealed class StaticOptionsMonitor<T> : IOptionsMonitor<T>
    {
        public StaticOptionsMonitor(T value)
        {
            CurrentValue = value;
        }

        public T CurrentValue { get; }

        public T Get(string? name) => CurrentValue;

        public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;

        private sealed class NullDisposable : IDisposable
        {
            public static readonly NullDisposable Instance = new();
            public void Dispose()
            {
            }
        }
    }

    private sealed class NoOpSecurityProvider : KF.Kafka.Configuration.Security.IKafkaSecurityProvider
    {
        public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
        {
            // No-op for integration tests (plaintext cluster).
        }
    }
}
