namespace KF.Kafka.Consumer.Backpressure;

public readonly record struct BackpressureContext(
    int WorkerId,
    int BacklogSize,
    int Capacity,
    double Utilization);
