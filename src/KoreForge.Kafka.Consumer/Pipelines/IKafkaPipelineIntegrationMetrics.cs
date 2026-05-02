using KoreForge.Kafka.Consumer.Batch;

namespace KoreForge.Kafka.Consumer.Pipelines;

public interface IKafkaPipelineIntegrationMetrics
{
    IKafkaPipelineIntegrationScope TrackBatch(string pipelineName, KafkaRecordBatch batch);
}

public interface IKafkaPipelineIntegrationScope : IDisposable
{
    void MarkFailed(Exception exception);
}

internal sealed class NoOpKafkaPipelineIntegrationMetrics : IKafkaPipelineIntegrationMetrics, IKafkaPipelineIntegrationScope
{
    public static readonly NoOpKafkaPipelineIntegrationMetrics Instance = new();

    private NoOpKafkaPipelineIntegrationMetrics()
    {
    }

    public IKafkaPipelineIntegrationScope TrackBatch(string pipelineName, KafkaRecordBatch batch) => this;

    public void MarkFailed(Exception exception)
    {
    }

    public void Dispose()
    {
    }
}
