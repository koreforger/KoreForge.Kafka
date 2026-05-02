using System;
using KoreForge.Kafka.Core.Runtime;

namespace KoreForge.Kafka.Consumer.Execution;

internal readonly record struct KafkaConsumerWorkerCreationContext(
    int WorkerIndex,
    ConsumerRuntimeRecord RuntimeRecord,
    Func<long> BeginNewWorkerInstance,
    Action<long> ResetFailures);
