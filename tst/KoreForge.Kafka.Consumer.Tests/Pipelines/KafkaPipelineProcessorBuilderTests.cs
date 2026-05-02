using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Kafka.Consumer.Pipelines;
using KoreForge.Kafka.Consumer.Tests.Support;
using KoreForge.Processing.Pipelines;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Pipelines;

public sealed class KafkaPipelineProcessorBuilderTests
{
    [Fact]
    public async Task Build_creates_processor_that_invokes_executor()
    {
        var executor = new RecordingExecutor();
        var builder = new KafkaPipelineProcessorBuilder<KafkaPipelineRecord>()
            .UseExecutor(executor)
            .ConfigureExecutionOptions(new PipelineExecutionOptions
            {
                IsSequential = true,
                MaxDegreeOfParallelism = 3
            })
            .UseContextFactory(() =>
            {
                var ctx = new PipelineContext();
                ctx.Set("scope", Guid.NewGuid());
                return ctx;
            })
            .UseContextInitializer((batch, ctx) => ctx.Set("batch-size", batch.Count));

        var processor = builder.Build(() => global::KoreForge.Processing.Pipelines.Pipeline.Start<KafkaPipelineRecord>().Build());
        var batch = KafkaRecordBatchFactory.CreateBatch(1, 2, 3);

        await processor.ProcessAsync(batch, CancellationToken.None);

        executor.Items.Should().NotBeNull();
        executor.Items!.Count.Should().Be(3);
        executor.Context.Should().NotBeNull();
        executor.Context!.Get<KafkaRecordBatch>(KafkaPipelineContextKeys.Batch).Should().BeSameAs(batch);
        executor.Context.Get<int>("batch-size").Should().Be(3);
        executor.Options.Should().NotBeNull();
        executor.Options!.IsSequential.Should().BeTrue();
        executor.Options.MaxDegreeOfParallelism.Should().Be(3);
    }

    private sealed class RecordingExecutor : IBatchPipelineExecutor<KafkaPipelineRecord, KafkaPipelineRecord>
    {
        public IReadOnlyList<KafkaPipelineRecord>? Items { get; private set; }
        public PipelineContext? Context { get; private set; }
        public PipelineExecutionOptions? Options { get; private set; }

        public Task ProcessBatchAsync(
            IReadOnlyList<KafkaPipelineRecord> items,
            IProcessingPipeline<KafkaPipelineRecord, KafkaPipelineRecord> pipeline,
            PipelineContext context,
            PipelineExecutionOptions options,
            CancellationToken cancellationToken)
        {
            Items = items;
            Context = context;
            Options = options;
            return Task.CompletedTask;
        }
    }
}
