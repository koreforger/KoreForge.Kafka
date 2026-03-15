namespace KF.Kafka.AdminClient.Instrumentation;

/// <summary>
/// Lightweight instrumentation surface so hosting applications can bridge to KF.Metrics or other systems.
/// </summary>
public interface IKafkaAdminMetrics
{
    IKafkaAdminCallScope TrackCall(string operation, string? topic, string? groupId);
    void RecordCacheHit(string cacheName);
    void RecordCacheMiss(string cacheName);
}

public interface IKafkaAdminCallScope : IDisposable
{
    void MarkError(Exception exception);
}

internal sealed class NoOpKafkaAdminMetrics : IKafkaAdminMetrics
{
    public static readonly NoOpKafkaAdminMetrics Instance = new();

    private NoOpKafkaAdminMetrics()
    {
    }

    public IKafkaAdminCallScope TrackCall(string operation, string? topic, string? groupId) => NoOpScope.Instance;

    public void RecordCacheHit(string cacheName)
    {
    }

    public void RecordCacheMiss(string cacheName)
    {
    }

    private sealed class NoOpScope : IKafkaAdminCallScope
    {
        public static readonly NoOpScope Instance = new();

        private NoOpScope()
        {
        }

        public void MarkError(Exception exception)
        {
        }

        public void Dispose()
        {
        }
    }
}
