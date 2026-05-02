using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Logging;
using KoreForge.Kafka.Producer.Metrics;
using KoreForge.Kafka.Producer.Pipelines;
using KoreForge.Time;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Producer.Buffer;

internal sealed class ProducerBuffer : IProducerBuffer
{
    private readonly IReadOnlyDictionary<string, TopicPipeline> _pipelines;
    private readonly IProducerBackpressurePolicy _backpressurePolicy;
    private readonly ProducerMetricsBridge _metrics;
    private readonly ISystemClock _clock;
    private readonly ILogger _logger;

    public ProducerBuffer(
        IReadOnlyDictionary<string, TopicPipeline> pipelines,
        IProducerBackpressurePolicy backpressurePolicy,
        ProducerMetricsBridge metrics,
        ISystemClock clock,
        ILogger logger)
    {
        _pipelines = pipelines ?? throw new ArgumentNullException(nameof(pipelines));
        _backpressurePolicy = backpressurePolicy ?? throw new ArgumentNullException(nameof(backpressurePolicy));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async ValueTask EnqueueAsync<TMessage>(
        string topicName,
        TMessage message,
        ProducerEnqueueOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
        {
            throw new ArgumentException("Topic name must be provided", nameof(topicName));
        }

        if (!_pipelines.TryGetValue(topicName, out var pipeline))
        {
            throw new InvalidOperationException($"Topic '{topicName}' is not configured for this producer.");
        }

        var resolvedTopic = options?.TopicOverride ?? pipeline.Definition.TopicName;
        var createdAt = _clock.Now;
        var headers = options?.Headers is null
            ? null
            : new Dictionary<string, string>(options.Headers, StringComparer.Ordinal);
        var envelope = new OutgoingEnvelope(
            message!,
            typeof(TMessage),
            resolvedTopic,
            options?.Key,
            headers,
            createdAt);

        await EnqueueInternalAsync(pipeline, envelope, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EnqueueInternalAsync(TopicPipeline pipeline, OutgoingEnvelope envelope, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (pipeline.Channel.Writer.TryWrite(envelope))
            {
                pipeline.Buffer.Increment();
                using var enqueueScope = _metrics.TrackEnqueue(pipeline.Definition.TopicName, pipeline.Buffer.Buffered, pipeline.Definition.ChannelCapacity);
                return;
            }

            var context = new ProducerBackpressureContext(
                pipeline.Definition.TopicName,
                pipeline.Buffer.Buffered,
                pipeline.Definition.ChannelCapacity,
                _clock.Now,
                pipeline.Definition.BackpressureMode);

            var decision = _backpressurePolicy.Evaluate(context);
            if (decision.ShouldDrop)
            {
                _logger.LogWarning(
                    ToEventId(ProducerLogEvents.Backpressure_Dropped),
                    "Dropping message enqueued at {Timestamp} for topic {Topic} due to backpressure",
                    envelope.LocalCreatedAt,
                    pipeline.Definition.TopicName);
                return;
            }

            if (decision.ShouldFail)
            {
                _logger.LogError(
                    ToEventId(ProducerLogEvents.Backpressure_Failed),
                    "Rejecting message for topic {Topic} because buffer is full",
                    pipeline.Definition.TopicName);
                throw new InvalidOperationException($"Producer buffer is full for topic {pipeline.Definition.TopicName}.");
            }

            _logger.LogInformation(
                ToEventId(ProducerLogEvents.Backpressure_Blocking),
                "Buffer full for topic {Topic}; waiting for capacity",
                pipeline.Definition.TopicName);

            await pipeline.Channel.Writer.WriteAsync(envelope, cancellationToken).ConfigureAwait(false);
            pipeline.Buffer.Increment();
            using var blockedScope = _metrics.TrackEnqueue(pipeline.Definition.TopicName, pipeline.Buffer.Buffered, pipeline.Definition.ChannelCapacity);
            return;
        }
    }

    private static EventId ToEventId(ProducerLogEvents evt) => new((int)evt, evt.ToString());
}
