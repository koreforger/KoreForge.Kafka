using System;
using System.Collections.Generic;
using System.Globalization;
using KF.Kafka.Consumer.Batch;
using KF.Metrics;
using KoreForge.Processing.Pipelines.Instrumentation;

namespace KF.Kafka.Consumer.Pipelines;

internal sealed class KafkaPipelineOperationMonitorMetrics : IPipelineMetrics, IKafkaPipelineIntegrationMetrics
{
    private readonly IOperationMonitor _monitor;

    public KafkaPipelineOperationMonitorMetrics(IOperationMonitor monitor)
    {
        _monitor = monitor ?? throw new ArgumentNullException(nameof(monitor));
    }

    public IPipelineBatchScope TrackBatch(string pipelineName, int recordCount, bool isSequential, int maxDegreeOfParallelism)
    {
        var tags = new Dictionary<string, string>
        {
            ["pipeline"] = pipelineName,
            ["records"] = recordCount.ToString(CultureInfo.InvariantCulture),
            ["mode"] = isSequential ? "sequential" : "parallel",
            ["dop"] = maxDegreeOfParallelism.ToString(CultureInfo.InvariantCulture)
        };

        var scope = _monitor.Begin("kafka.pipeline.batch", new OperationTags(tags));
        return new PipelineBatchScope(_monitor, scope, pipelineName);
    }

    public IKafkaPipelineIntegrationScope TrackBatch(string pipelineName, KafkaRecordBatch batch)
    {
        var tags = new Dictionary<string, string>
        {
            ["pipeline"] = pipelineName,
            ["batch.size"] = batch.Count.ToString(CultureInfo.InvariantCulture)
        };

        if (batch.Count > 0)
        {
            var first = batch.Records[0];
            tags["topic"] = first.Topic;
            tags["partition"] = first.Partition.Value.ToString(CultureInfo.InvariantCulture);
        }

        var scope = _monitor.Begin("kafka.consumer.pipeline.batch", new OperationTags(tags));
        return new IntegrationScope(scope);
    }

    private sealed class PipelineBatchScope : IPipelineBatchScope
    {
        private readonly IOperationMonitor _monitor;
        private readonly OperationScope _scope;
        private readonly string _pipelineName;

        public PipelineBatchScope(IOperationMonitor monitor, OperationScope scope, string pipelineName)
        {
            _monitor = monitor;
            _scope = scope;
            _pipelineName = pipelineName;
        }

        public IPipelineStepScope TrackStep(string stepName, int recordCount, bool isBatchAware)
        {
            var tags = new Dictionary<string, string>
            {
                ["pipeline"] = _pipelineName,
                ["step"] = stepName,
                ["records"] = recordCount.ToString(CultureInfo.InvariantCulture),
                ["mode"] = isBatchAware ? "batch" : "record"
            };

            var scope = _monitor.Begin("kafka.pipeline.step", new OperationTags(tags));
            return new PipelineStepScope(_monitor, scope, _pipelineName, stepName);
        }

        public void MarkFailed(Exception exception)
        {
            _scope.MarkFailed();
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }

    private sealed class PipelineStepScope : IPipelineStepScope
    {
        private readonly IOperationMonitor _monitor;
        private readonly OperationScope _scope;
        private readonly string _pipelineName;
        private readonly string _stepName;

        public PipelineStepScope(IOperationMonitor monitor, OperationScope scope, string pipelineName, string stepName)
        {
            _monitor = monitor;
            _scope = scope;
            _pipelineName = pipelineName;
            _stepName = stepName;
        }

        public void RecordOutcome(int abortedCount)
        {
            if (abortedCount <= 0)
            {
                return;
            }

            var tags = new Dictionary<string, string>
            {
                ["pipeline"] = _pipelineName,
                ["step"] = _stepName,
                ["aborted"] = abortedCount.ToString(CultureInfo.InvariantCulture)
            };

            using var abortScope = _monitor.Begin("kafka.pipeline.step.abort", new OperationTags(tags));
        }

        public void MarkFailed(Exception exception)
        {
            _scope.MarkFailed();
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }

    private sealed class IntegrationScope : IKafkaPipelineIntegrationScope
    {
        private readonly OperationScope _scope;

        public IntegrationScope(OperationScope scope)
        {
            _scope = scope;
        }

        public void MarkFailed(Exception exception)
        {
            _scope.MarkFailed();
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }
}
