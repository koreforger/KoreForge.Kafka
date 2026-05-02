using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Buffer;
using KoreForge.Kafka.Producer.Logging;
using KoreForge.Kafka.Producer.Metrics;
using KoreForge.Kafka.Producer.Pipelines;
using KoreForge.Kafka.Producer.Tests.Support;
using KoreForge.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Buffer;

public sealed class ProducerBufferTests
{
    [Fact]
    public async Task Enqueue_Drops_WhenPolicyRequestsDrop()
    {
        var pipeline = CreatePipeline(capacity: 1, mode: ProducerBackpressureMode.Drop);
        // Fill pipeline to force backpressure
        pipeline.Channel.Writer.TryWrite(CreateEnvelope());
        pipeline.Buffer.Increment();

        var buffer = CreateBuffer(pipeline, new StaticPolicy(ProducerBackpressureDecision.Drop()));
        Func<Task> act = () => buffer.EnqueueAsync("topic-a", "payload", cancellationToken: CancellationToken.None).AsTask();
        await act.Should().NotThrowAsync();

        pipeline.Buffer.Buffered.Should().Be(1);
    }

    [Fact]
    public async Task Enqueue_Throws_WhenPolicyFails()
    {
        var pipeline = CreatePipeline(capacity: 1, mode: ProducerBackpressureMode.Fail);
        pipeline.Channel.Writer.TryWrite(CreateEnvelope());
        pipeline.Buffer.Increment();

        var buffer = CreateBuffer(pipeline, new StaticPolicy(ProducerBackpressureDecision.Fail()));
        Func<Task> act = () => buffer.EnqueueAsync("topic-a", "payload", cancellationToken: CancellationToken.None).AsTask();
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public async Task Enqueue_Throws_WhenTopicNameMissing(string? topicName)
    {
        var pipeline = CreatePipeline(capacity: 1, mode: ProducerBackpressureMode.Block);
        var buffer = CreateBuffer(pipeline, new StaticPolicy(ProducerBackpressureDecision.Block()));

        Func<Task> act = () => buffer.EnqueueAsync(topicName!, "payload", cancellationToken: CancellationToken.None).AsTask();

        await act.Should().ThrowAsync<ArgumentException>()
            .WithParameterName("topicName");
    }

    [Fact]
    public async Task Enqueue_Throws_WhenTopicNotConfigured()
    {
        var pipelines = new Dictionary<string, TopicPipeline>(StringComparer.OrdinalIgnoreCase);
        var metrics = new ProducerMetricsBridge(null);
        var buffer = new ProducerBuffer(
            pipelines,
            new StaticPolicy(ProducerBackpressureDecision.Block()),
            metrics,
            SystemClock.Instance,
            NullLoggerFactory.Instance.CreateLogger<ProducerBuffer>());

        Func<Task> act = () => buffer.EnqueueAsync("missing-topic", "payload", cancellationToken: CancellationToken.None).AsTask();

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Topic 'missing-topic' is not configured for this producer.");
    }

    [Fact]
    public async Task Enqueue_UsesOptionsToBuildEnvelope()
    {
        var pipeline = CreatePipeline(capacity: 2, mode: ProducerBackpressureMode.Block);
        var buffer = CreateBuffer(pipeline, new StaticPolicy(ProducerBackpressureDecision.Block()));
        var options = new ProducerEnqueueOptions
        {
            TopicOverride = "topic-override",
            Key = "order-42",
            Headers = new Dictionary<string, string>
            {
                ["trace-id"] = "abc",
                ["tenant"] = "unit-test"
            }
        };

        await buffer.EnqueueAsync("topic-a", "payload", options, CancellationToken.None);

        pipeline.Channel.Reader.TryRead(out var envelope).Should().BeTrue();
        envelope!.Topic.Should().Be(options.TopicOverride);
        envelope.Key.Should().Be(options.Key);
        envelope.Payload.Should().Be("payload");
        envelope.Headers.Should().NotBeNull();
        envelope.Headers!["tenant"].Should().Be("unit-test");

        options.Headers!["tenant"] = "mutated";
        envelope.Headers["tenant"].Should().Be("unit-test");
    }

    [Fact]
    public async Task Enqueue_WaitsAndLogs_WhenPolicyBlocks()
    {
        var pipeline = CreatePipeline(capacity: 1, mode: ProducerBackpressureMode.Block);
        pipeline.Channel.Writer.TryWrite(CreateEnvelope());
        pipeline.Buffer.Increment();

        using var loggerFactory = new RecordingLoggerFactory();
        var logger = loggerFactory.CreateLogger<ProducerBuffer>();
        var buffer = CreateBuffer(pipeline, new StaticPolicy(ProducerBackpressureDecision.Block()), logger: logger);

        var enqueueTask = buffer.EnqueueAsync("topic-a", "payload", cancellationToken: CancellationToken.None).AsTask();

        await Task.Delay(50);
        pipeline.Channel.Reader.TryRead(out _).Should().BeTrue();
        pipeline.Buffer.Decrement();

        await enqueueTask;

        pipeline.Buffer.Buffered.Should().Be(1);
        pipeline.Channel.Reader.TryRead(out var enqueued).Should().BeTrue();
        enqueued!.Payload.Should().Be("payload");
        pipeline.Buffer.Decrement();

        loggerFactory.Entries.Should().Contain(entry => entry.EventId.Id == (int)ProducerLogEvents.Backpressure_Blocking);
    }

    private static TopicPipeline CreatePipeline(int capacity, ProducerBackpressureMode mode)
    {
        var definition = new ProducerTopicDefinition("topic-a", 1, capacity, mode, new ProducerBacklogSettings());
        return new TopicPipeline(definition);
    }

    private static ProducerBuffer CreateBuffer(
        TopicPipeline pipeline,
        IProducerBackpressurePolicy policy,
        ProducerMetricsBridge? metrics = null,
        ISystemClock? clock = null,
        ILogger? logger = null)
    {
        var dict = new Dictionary<string, TopicPipeline>(StringComparer.OrdinalIgnoreCase)
        {
            [pipeline.Definition.TopicName] = pipeline
        };
        metrics ??= new ProducerMetricsBridge(null);
        clock ??= SystemClock.Instance;
        logger ??= NullLoggerFactory.Instance.CreateLogger<ProducerBuffer>();
        return new ProducerBuffer(dict, policy, metrics, clock, logger);
    }

    private static OutgoingEnvelope CreateEnvelope()
    {
        return new OutgoingEnvelope("seed", typeof(string), "topic-a", null, null, DateTimeOffset.UtcNow);
    }

    private sealed class StaticPolicy : IProducerBackpressurePolicy
    {
        private readonly ProducerBackpressureDecision _decision;

        public StaticPolicy(ProducerBackpressureDecision decision)
        {
            _decision = decision;
        }

        public ProducerBackpressureDecision Evaluate(ProducerBackpressureContext context) => _decision;
    }
}
