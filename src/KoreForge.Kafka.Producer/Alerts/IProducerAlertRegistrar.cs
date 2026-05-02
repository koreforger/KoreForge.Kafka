using System;
using KoreForge.Kafka.Core.Alerts;
using KoreForge.Kafka.Producer.Runtime;

namespace KoreForge.Kafka.Producer.Alerts;

public interface IProducerAlertRegistrar
{
    IProducerAlertRegistrar RegisterAlert(
        Func<ProducerStatusSnapshot, bool> condition,
        Action<ProducerStatusSnapshot> action,
        AlertOptions? options = null);
}
