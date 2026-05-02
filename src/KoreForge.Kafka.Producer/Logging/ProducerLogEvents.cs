using KoreForge.Logging;

namespace KoreForge.Kafka.Producer.Logging;

[LogEventSource(LoggerRootTypeName = "KafkaProducerLogs", BasePath = "kafka.producer")]
public enum ProducerLogEvents
{
    Host_Status_Starting = 2000,
    Host_Status_Started = 2001,
    Host_Status_Stopping = 2002,
    Host_Status_Stopped = 2003,

    Topic_Status_Starting = 2100,
    Topic_Status_Started = 2101,
    Topic_Status_Stopping = 2102,
    Topic_Status_Stopped = 2103,

    Backpressure_Blocking = 2200,
    Backpressure_Dropped = 2201,
    Backpressure_Failed = 2202,

    Producer_Send_Succeeded = 2300,
    Producer_Send_Failed = 2301,

    Backlog_Persisting = 2400,
    Backlog_Persisted = 2401,
    Backlog_Restoring = 2402,
    Backlog_Restored = 2403,

    Restart_Scheduled = 2500,
    Restart_GiveUp = 2501,
    Restart_CircuitBreaker = 2502
}
