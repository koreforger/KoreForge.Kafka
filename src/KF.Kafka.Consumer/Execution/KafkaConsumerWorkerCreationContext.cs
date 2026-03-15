using System;
using KF.Kafka.Core.Runtime;

namespace KF.Kafka.Consumer.Execution;

internal readonly record struct KafkaConsumerWorkerCreationContext(
    int WorkerIndex,
    ConsumerRuntimeRecord RuntimeRecord,
    Func<long> BeginNewWorkerInstance,
    Action<long> ResetFailures);
