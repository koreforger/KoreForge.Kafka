using System;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Producer.Abstractions;
using Microsoft.Extensions.Logging;

namespace KF.Kafka.Producer.Backpressure;

public sealed class DefaultProducerBackpressurePolicy : IProducerBackpressurePolicy
{
    private readonly ILogger _logger;

    public DefaultProducerBackpressurePolicy(ILogger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public ProducerBackpressureDecision Evaluate(ProducerBackpressureContext context)
    {
        return context.Mode switch
        {
            ProducerBackpressureMode.Block => ProducerBackpressureDecision.Block(),
            ProducerBackpressureMode.Drop => ProducerBackpressureDecision.Drop(),
            ProducerBackpressureMode.Fail => ProducerBackpressureDecision.Fail(),
            _ => ProducerBackpressureDecision.Block()
        };
    }

    public void LogDecision(ProducerBackpressureContext context, ProducerBackpressureDecision decision)
    {
        if (!decision.ShouldDrop && !decision.ShouldFail)
        {
            return;
        }

        var level = decision.ShouldFail ? LogLevel.Error : LogLevel.Warning;
        _logger.Log(level,
            "Producer backpressure decision for {Topic} buffered={Buffered} capacity={Capacity} decision={Decision}",
            context.TopicName,
            context.BufferedMessages,
            context.BufferCapacity,
            decision);
    }
}
