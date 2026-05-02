using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Abstractions;

namespace KoreForge.Kafka.Producer.Backlog
{
    public sealed class ProducerBacklogPersistContext
    {
        public ProducerBacklogPersistContext(string topicName, IReadOnlyList<OutgoingEnvelope> envelopes, ProducerBacklogSettings settings)
        {
            TopicName = topicName;
            Envelopes = envelopes;
            Settings = settings;
        }

        public string TopicName { get; }
        public IReadOnlyList<OutgoingEnvelope> Envelopes { get; }
        public ProducerBacklogSettings Settings { get; }
    }

    public sealed class ProducerBacklogRestoreContext
    {
        public ProducerBacklogRestoreContext(string topicName, ProducerBacklogSettings settings)
        {
            TopicName = topicName;
            Settings = settings;
        }

        public string TopicName { get; }
        public ProducerBacklogSettings Settings { get; }
    }

    public interface IProducerBacklogPersistenceStrategy
    {
        Task PersistAsync(ProducerBacklogPersistContext context, CancellationToken cancellationToken);
        Task<IReadOnlyList<OutgoingEnvelope>> RestoreAsync(ProducerBacklogRestoreContext context, CancellationToken cancellationToken);
    }
}
