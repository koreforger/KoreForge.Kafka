using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Buffer;
using KoreForge.Kafka.Producer.Logging;
using KoreForge.Kafka.Producer.Metrics;
using KoreForge.Kafka.Producer.Runtime;
using KoreForge.Time;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Producer.Workers;

internal sealed class ProducerWorker : IAsyncDisposable
{
    private readonly int _workerId;
    private readonly string _topicName;
    private readonly ChannelReader<OutgoingEnvelope> _channel;
    private readonly TopicBufferState _bufferState;
    private readonly IProducerSerializerRegistry _serializerRegistry;
    private readonly ProducerRuntimeRecord _runtimeRecord;
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILogger _logger;
    private readonly ProducerMetricsBridge _metrics;
    private readonly ISystemClock _clock;
    private readonly CancellationTokenSource _cts = new();

    private Task _execution = Task.CompletedTask;

    public ProducerWorker(
        int workerId,
        string topicName,
        ChannelReader<OutgoingEnvelope> channel,
        TopicBufferState bufferState,
        IProducerSerializerRegistry serializerRegistry,
        ProducerRuntimeRecord runtimeRecord,
        IProducer<byte[], byte[]> producer,
        ILogger logger,
        ProducerMetricsBridge metrics,
        ISystemClock clock)
    {
        _workerId = workerId;
        _topicName = topicName;
        _channel = channel;
        _bufferState = bufferState;
        _serializerRegistry = serializerRegistry;
        _runtimeRecord = runtimeRecord;
        _producer = producer;
        _logger = logger;
        _metrics = metrics;
        _clock = clock;
    }

    public Task Execution => _execution;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, cancellationToken);
        _execution = Task.Run(() => RunAsync(linked.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _channel.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_channel.TryRead(out var envelope))
                {
                    _bufferState.Decrement();
                    await SendAsync(envelope, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // ignored
        }
    }

    private async Task SendAsync(OutgoingEnvelope envelope, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        using var scope = _metrics.TrackSend(_topicName, 1);
        try
        {
            var record = _serializerRegistry.Serialize(envelope);
#pragma warning disable CS8601
            var message = new Message<byte[], byte[]>
            {
                Key = record.KeyBytes,
#pragma warning restore CS8601
                Value = record.ValueBytes,
                Headers = BuildHeaders(record.Headers),
                Timestamp = new Timestamp(envelope.LocalCreatedAt.UtcDateTime, TimestampType.CreateTime)
            };

            await _producer.ProduceAsync(record.Topic, message, cancellationToken).ConfigureAwait(false);
            stopwatch.Stop();
            _runtimeRecord.RecordBatch(1, stopwatch.Elapsed, envelope.LocalCreatedAt.LocalDateTime);
            _logger.LogDebug(
                ToEventId(ProducerLogEvents.Producer_Send_Succeeded),
                "Worker {Worker} produced message to {Topic}",
                _workerId,
                record.Topic);
        }
        catch (Exception ex)
        {
            scope.MarkFailed();
            stopwatch.Stop();
            _runtimeRecord.RecordFailure();
            _logger.LogError(
                ToEventId(ProducerLogEvents.Producer_Send_Failed),
                ex,
                "Worker {Worker} failed to send message for topic {Topic}",
                _workerId,
                _topicName);
            throw;
        }
    }

    private static Headers? BuildHeaders(IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (headers is null)
        {
            return null;
        }

        var result = new Headers();
        foreach (var kvp in headers)
        {
            result.Add(kvp.Key, kvp.Value);
        }

        return result;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try
        {
            await _execution.ConfigureAwait(false);
        }
        catch
        {
            // Ignore failures during shutdown
        }
        finally
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
            _producer.Dispose();
        }
    }

    private static EventId ToEventId(ProducerLogEvents evt) => new((int)evt, evt.ToString());
}
