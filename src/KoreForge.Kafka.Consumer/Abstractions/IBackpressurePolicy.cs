using KoreForge.Kafka.Consumer.Backpressure;

namespace KoreForge.Kafka.Consumer.Abstractions;

/// <summary>
/// Determines whether a consumer worker should pause or resume consumption based on backlog state.
/// </summary>
public interface IBackpressurePolicy
{
    BackpressureDecision Evaluate(BackpressureContext context);
}
