using System;
using System.Collections.Generic;
using Confluent.Kafka;
using KF.Kafka.Configuration.Options;

namespace KF.Kafka.Configuration.Runtime;

public sealed class KafkaConsumerRuntimeConfig
{
    public ConsumerConfig ConfluentConfig { get; }
    public ExtendedConsumerSettings Extended { get; }
    public IReadOnlyCollection<string> Topics { get; }
    public KafkaSecuritySettings Security { get; }

    public KafkaConsumerRuntimeConfig(
        ConsumerConfig confluentConfig,
        ExtendedConsumerSettings extended,
        IReadOnlyCollection<string> topics,
        KafkaSecuritySettings security)
    {
        ConfluentConfig = confluentConfig ?? throw new ArgumentNullException(nameof(confluentConfig));
        Extended = extended ?? throw new ArgumentNullException(nameof(extended));
        Topics = topics ?? Array.Empty<string>();
        Security = security ?? new KafkaSecuritySettings();
    }
}

public sealed class KafkaProducerRuntimeConfig
{
    public ProducerConfig ConfluentConfig { get; }
    public ExtendedProducerSettings Extended { get; }
    public IReadOnlyList<string> Topics { get; }

    public KafkaProducerRuntimeConfig(
        ProducerConfig confluentConfig,
        ExtendedProducerSettings extended,
        IReadOnlyList<string> topics)
    {
        ConfluentConfig = confluentConfig ?? throw new ArgumentNullException(nameof(confluentConfig));
        Extended = extended ?? throw new ArgumentNullException(nameof(extended));
        Topics = topics ?? Array.Empty<string>();
    }
}

public sealed class KafkaAdminRuntimeConfig
{
    public AdminClientConfig ConfluentConfig { get; }
    public ExtendedAdminSettings Extended { get; }

    public KafkaAdminRuntimeConfig(
        AdminClientConfig confluentConfig,
        ExtendedAdminSettings extended)
    {
        ConfluentConfig = confluentConfig ?? throw new ArgumentNullException(nameof(confluentConfig));
        Extended = extended ?? throw new ArgumentNullException(nameof(extended));
    }
}
