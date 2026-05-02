using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Batch;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Processing.Pipelines;
using KoreForge.Processing.Pipelines.Instrumentation;

namespace KoreForge.Kafka.Consumer.Pipelines;

/// <summary>
/// Fluent configuration surface for wiring a processing pipeline into the Kafka consumer host.
/// </summary>
public sealed class KafkaPipelineProcessorBuilder<TOut>
{
    private Func<IBatchPipelineExecutor<KafkaPipelineRecord, TOut>>? _executorFactory;
    private Func<PipelineExecutionOptions> _executionOptionsFactory = static () => new PipelineExecutionOptions();
    private bool _executionOptionsExplicitlySet;
    private Func<PipelineContext> _contextFactory = static () => new PipelineContext();
    private Action<KafkaRecordBatch, PipelineContext>? _contextInitializer;
    private string _pipelineName = "kafka-consumer-pipeline";
    private Func<IPipelineMetrics> _metricsFactory = static () => NoOpPipelineMetrics.Instance;
    private IKafkaPipelineIntegrationMetrics _integrationMetrics = NoOpKafkaPipelineIntegrationMetrics.Instance;

    public KafkaPipelineProcessorBuilder<TOut> UseExecutor(Func<IBatchPipelineExecutor<KafkaPipelineRecord, TOut>> factory)
    {
        _executorFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UseExecutor(IBatchPipelineExecutor<KafkaPipelineRecord, TOut> executor)
    {
        if (executor is null)
        {
            throw new ArgumentNullException(nameof(executor));
        }

        _executorFactory = () => executor;
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> ConfigureExecutionOptions(PipelineExecutionOptions options)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        _executionOptionsExplicitlySet = true;
        _executionOptionsFactory = () => options;
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> ConfigureExecutionOptions(Func<PipelineExecutionOptions> factory)
    {
        _executionOptionsExplicitlySet = true;
        _executionOptionsFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UseContextFactory(Func<PipelineContext> factory)
    {
        _contextFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UseContextInitializer(Action<KafkaRecordBatch, PipelineContext> initializer)
    {
        _contextInitializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UsePipelineName(string pipelineName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pipelineName);
        _pipelineName = pipelineName;
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UsePipelineMetrics(IPipelineMetrics metrics)
    {
        if (metrics is null)
        {
            throw new ArgumentNullException(nameof(metrics));
        }

        _metricsFactory = () => metrics;
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UsePipelineMetrics(Func<IPipelineMetrics> factory)
    {
        _metricsFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    public KafkaPipelineProcessorBuilder<TOut> UseIntegrationMetrics(IKafkaPipelineIntegrationMetrics metrics)
    {
        _integrationMetrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        return this;
    }

    internal void ApplyConsumerSettings(ExtendedConsumerSettings settings)
    {
        // Only override execution options if the caller has not explicitly configured them.
        if (!_executionOptionsExplicitlySet)
        {
            var dop = Math.Max(1, settings.PipelineMaxDegreeOfParallelism);
            _executionOptionsFactory = () => new PipelineExecutionOptions
            {
                // Sequential mode when DOP=1 skips the parallel task machinery entirely.
                IsSequential = dop == 1,
                MaxDegreeOfParallelism = dop,
            };
        }
    }

    internal IKafkaBatchProcessor Build(Func<IProcessingPipeline<KafkaPipelineRecord, TOut>> pipelineFactory)
    {
        if (pipelineFactory is null)
        {
            throw new ArgumentNullException(nameof(pipelineFactory));
        }

        var pipeline = pipelineFactory();
        var metrics = _metricsFactory() ?? throw new InvalidOperationException("Pipeline metrics factory returned null.");
        var executor = _executorFactory?.Invoke() ?? new BatchPipelineExecutor<KafkaPipelineRecord, TOut>(_pipelineName, metrics);
        var configuredOptions = _executionOptionsFactory();
        if (configuredOptions is null)
        {
            throw new InvalidOperationException("Execution options factory returned null.");
        }

        var options = new PipelineExecutionOptions
        {
            IsSequential = configuredOptions.IsSequential,
            MaxDegreeOfParallelism = configuredOptions.MaxDegreeOfParallelism
        };

        var integrationMetrics = _integrationMetrics ?? NoOpKafkaPipelineIntegrationMetrics.Instance;
        return new KafkaPipelineBatchProcessor<TOut>(_pipelineName, executor, pipeline, options, _contextFactory, _contextInitializer, integrationMetrics);
    }
}
