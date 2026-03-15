using System;
using KF.Kafka.Core.Alerts;
using KF.Kafka.Producer.Runtime;

namespace KF.Kafka.Producer.Alerts;

public interface IProducerAlertRegistrar
{
    IProducerAlertRegistrar RegisterAlert(
        Func<ProducerStatusSnapshot, bool> condition,
        Action<ProducerStatusSnapshot> action,
        AlertOptions? options = null);
}
