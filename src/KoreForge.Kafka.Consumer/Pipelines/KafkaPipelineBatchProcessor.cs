using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Processing.Pipelines;

namespace KoreForge.Kafka.Consumer.Pipelines;

internal sealed class KafkaPipelineBatchProcessor<TOut> : IKafkaBatchProcessor
{
    private readonly string _pipelineName;
    private readonly IBatchPipelineExecutor<KafkaPipelineRecord, TOut> _executor;
    private readonly IProcessingPipeline<KafkaPipelineRecord, TOut> _pipeline;
    private readonly PipelineExecutionOptions _options;
    private readonly Func<PipelineContext> _contextFactory;
    private readonly Action<KafkaRecordBatch, PipelineContext>? _contextInitializer;
    private readonly IKafkaPipelineIntegrationMetrics _integrationMetrics;

    public KafkaPipelineBatchProcessor(
        string pipelineName,
        IBatchPipelineExecutor<KafkaPipelineRecord, TOut> executor,
        IProcessingPipeline<KafkaPipelineRecord, TOut> pipeline,
        PipelineExecutionOptions options,
        Func<PipelineContext> contextFactory,
        Action<KafkaRecordBatch, PipelineContext>? contextInitializer,
        IKafkaPipelineIntegrationMetrics integrationMetrics)
    {
        _pipelineName = pipelineName ?? throw new ArgumentNullException(nameof(pipelineName));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _pipeline = pipeline ?? throw new ArgumentNullException(nameof(pipeline));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _contextFactory = contextFactory ?? throw new ArgumentNullException(nameof(contextFactory));
        _contextInitializer = contextInitializer;
        _integrationMetrics = integrationMetrics ?? throw new ArgumentNullException(nameof(integrationMetrics));
    }

    public async Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
    {
        if (batch is null)
        {
            throw new ArgumentNullException(nameof(batch));
        }

        var context = _contextFactory();
        if (context is null)
        {
            throw new InvalidOperationException("Pipeline context factory returned null.");
        }

        context.Set(KafkaPipelineContextKeys.Batch, batch);
        _contextInitializer?.Invoke(batch, context);

        using var scope = _integrationMetrics.TrackBatch(_pipelineName, batch);

        try
        {
            var records = CreateInput(batch);
            await _executor.ProcessBatchAsync(records, _pipeline, context, _options, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            scope.MarkFailed(ex);
            throw;
        }
    }

    private static KafkaPipelineRecord[] CreateInput(KafkaRecordBatch batch)
    {
        var items = new KafkaPipelineRecord[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            items[i] = KafkaPipelineRecord.FromConsumeResult(batch.Records[i]);
        }

        return items;
    }
}
