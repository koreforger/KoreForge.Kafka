using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Kafka.Consumer.Pipelines;
using KoreForge.Kafka.Consumer.Tests.Support;
using KoreForge.Processing.Pipelines;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Pipelines;

public sealed class KafkaPipelineBatchProcessorTests
{
    [Fact]
    public async Task ProcessAsync_InvokesExecutorAndInitializer()
    {
        var batch = KafkaRecordBatchFactory.CreateBatch(10, 11, 12);
        var executor = new RecordingExecutor();
        var pipeline = new DummyPipeline();
        var metrics = new RecordingIntegrationMetrics();
        var initializerCalls = 0;
        var processor = CreateProcessor(
            executor,
            pipeline,
            metrics,
            (b, ctx) =>
            {
                initializerCalls++;
                ctx.Set("custom-flag", true);
                b.Should().BeSameAs(batch);
            });

        await processor.ProcessAsync(batch, CancellationToken.None);

        executor.ReceivedRecords.Should().NotBeNull();
        executor.ReceivedRecords.Should().HaveCount(batch.Count);
        executor.ReceivedPipeline.Should().BeSameAs(pipeline);
        executor.ReceivedContext.Should().NotBeNull();
        executor.ReceivedContext!.Get<KafkaRecordBatch>(KafkaPipelineContextKeys.Batch)
            .Should().BeSameAs(batch);
        executor.ReceivedOptions.Should().NotBeNull();
        initializerCalls.Should().Be(1);
        metrics.LastPipeline.Should().Be("consumer-test-pipeline");
        metrics.LastBatch.Should().BeSameAs(batch);
        metrics.Scope.Disposed.Should().BeTrue();
        pipeline.ProcessedRecords.Should().HaveCount(batch.Count);
        pipeline.ProcessedRecords.Should().AllSatisfy(record => record.Topic.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task ProcessAsync_Throws_WhenBatchIsNull()
    {
        var processor = CreateProcessor();

        var act = async () => await processor.ProcessAsync(batch: null!, CancellationToken.None);

        var exception = await act.Should().ThrowAsync<ArgumentNullException>();
        exception.And.ParamName.Should().Be("batch");
    }

    [Fact]
    public async Task ProcessAsync_Throws_WhenContextFactoryReturnsNull()
    {
        var batch = KafkaRecordBatchFactory.CreateBatch(1);
        var processor = CreateProcessor(contextFactory: () => null!);

        var act = async () => await processor.ProcessAsync(batch, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Pipeline context factory returned null.");
    }

    [Fact]
    public async Task ProcessAsync_MarksIntegrationScopeOnFailure()
    {
        var batch = KafkaRecordBatchFactory.CreateBatch(5);
        var executor = new RecordingExecutor { ThrowOnExecute = true };
        var metrics = new RecordingIntegrationMetrics();
        var processor = CreateProcessor(executor: executor, metrics: metrics);

        var act = async () => await processor.ProcessAsync(batch, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("executor failure");
        metrics.Scope.Failure.Should().NotBeNull();
        metrics.Scope.Disposed.Should().BeTrue();
    }

    private static KafkaPipelineBatchProcessor<string> CreateProcessor(
        RecordingExecutor? executor = null,
        DummyPipeline? pipeline = null,
        RecordingIntegrationMetrics? metrics = null,
        Action<KafkaRecordBatch, PipelineContext>? initializer = null,
        Func<PipelineContext>? contextFactory = null)
    {
        return new KafkaPipelineBatchProcessor<string>(
            pipelineName: "consumer-test-pipeline",
            executor: executor ?? new RecordingExecutor(),
            pipeline: pipeline ?? new DummyPipeline(),
            options: new PipelineExecutionOptions { IsSequential = true, MaxDegreeOfParallelism = 1 },
            contextFactory: contextFactory ?? (() => new PipelineContext()),
            contextInitializer: initializer,
            integrationMetrics: metrics ?? new RecordingIntegrationMetrics());
    }

    private sealed class RecordingExecutor : IBatchPipelineExecutor<KafkaPipelineRecord, string>
    {
        public IReadOnlyList<KafkaPipelineRecord>? ReceivedRecords { get; private set; }
        public IProcessingPipeline<KafkaPipelineRecord, string>? ReceivedPipeline { get; private set; }
        public PipelineContext? ReceivedContext { get; private set; }
        public PipelineExecutionOptions? ReceivedOptions { get; private set; }
        public bool ThrowOnExecute { get; set; }

        public async Task ProcessBatchAsync(
            IReadOnlyList<KafkaPipelineRecord> items,
            IProcessingPipeline<KafkaPipelineRecord, string> pipeline,
            PipelineContext context,
            PipelineExecutionOptions options,
            CancellationToken cancellationToken)
        {
            ReceivedRecords = items;
            ReceivedPipeline = pipeline;
            ReceivedContext = context;
            ReceivedOptions = options;

            if (ThrowOnExecute)
            {
                throw new InvalidOperationException("executor failure");
            }

            // Simulate the executor invoking the pipeline for each record.
            foreach (var item in items)
            {
                await pipeline.ProcessAsync(item, context, cancellationToken);
            }
        }
    }

    private sealed class DummyPipeline : IProcessingPipeline<KafkaPipelineRecord, string>
    {
        public List<KafkaPipelineRecord> ProcessedRecords { get; } = new();

        public ValueTask<StepOutcome<string>> ProcessAsync(
            KafkaPipelineRecord input,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            ProcessedRecords.Add(input);
            return ValueTask.FromResult(StepOutcome<string>.Continue($"offset-{input.Offset}"));
        }
    }

    private sealed class RecordingIntegrationMetrics : IKafkaPipelineIntegrationMetrics
    {
        public TrackingScope Scope { get; } = new();
        public string? LastPipeline { get; private set; }
        public KafkaRecordBatch? LastBatch { get; private set; }

        public IKafkaPipelineIntegrationScope TrackBatch(string pipelineName, KafkaRecordBatch batch)
        {
            LastPipeline = pipelineName;
            LastBatch = batch;
            return Scope;
        }

        public sealed class TrackingScope : IKafkaPipelineIntegrationScope
        {
            public bool Disposed { get; private set; }
            public Exception? Failure { get; private set; }

            public void MarkFailed(Exception exception)
            {
                Failure = exception;
            }

            public void Dispose()
            {
                Disposed = true;
            }
        }
    }
}
