using System;
using System.Threading;
using System.Threading.Tasks;

namespace KoreForge.Kafka.Consumer.Execution;

internal interface IKafkaConsumerWorker : IAsyncDisposable
{
    int WorkerId { get; }
    Task Execution { get; }
    Task StartAsync(CancellationToken supervisorToken);
    Task StopAsync(CancellationToken cancellationToken);
}
