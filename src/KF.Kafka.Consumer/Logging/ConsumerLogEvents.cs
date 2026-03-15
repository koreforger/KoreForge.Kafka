using KF.Logging;

namespace KF.Kafka.Consumer.Logging;

[LogEventSource(LoggerRootTypeName = "KafkaConsumerLogs", BasePath = "kafka.consumer")]
public enum ConsumerLogEvents
{
    Host_Status_Starting = 1000,
    Host_Status_Started = 1001,
    Host_Status_Stopping = 1010,
    Host_Status_Stopped = 1011,

    Worker_Status_Starting = 1100,
    Worker_Status_Started = 1101,
    Worker_Status_Stopping = 1102,
    Worker_Status_Stopped = 1103,

    Worker_Batch_Dispatched = 1150,
    Worker_Batch_Failed = 1151,
    Worker_Consume_Error = 1160,
    Worker_Assignment_Error = 1161,
    Worker_Restart_Scheduled = 1170,
    Worker_Restart_GiveUp = 1171,
    Worker_Backpressure_Decision = 1180,
    Worker_Backpressure_Paused = 1181,
    Worker_Backpressure_Resumed = 1182,
    Worker_Backpressure_Deferred = 1183,

    Certificates_Isolation_Configured = 1300,
    Certificates_Isolation_Created = 1301,
    Certificates_Isolation_CleanupFailed = 1302
}
