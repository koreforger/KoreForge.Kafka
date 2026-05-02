using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Producer.Abstractions;
using KoreForge.Kafka.Producer.Runtime;
using Microsoft.Extensions.Logging;

namespace KoreForge.Kafka.Producer.Backlog;

public sealed class FileProducerBacklogPersistenceStrategy : IProducerBacklogPersistenceStrategy, IProducerBacklogDiagnostics
{
    private readonly ILogger<FileProducerBacklogPersistenceStrategy> _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ConcurrentDictionary<string, ProducerBacklogTopicStatus> _topicStatuses = new(StringComparer.OrdinalIgnoreCase);

    public FileProducerBacklogPersistenceStrategy(ILogger<FileProducerBacklogPersistenceStrategy> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false
        };
    }

    public async Task PersistAsync(ProducerBacklogPersistContext context, CancellationToken cancellationToken)
    {
        if (!context.Settings.PersistenceEnabled || context.Envelopes.Count == 0)
        {
            return;
        }

        Directory.CreateDirectory(context.Settings.Directory);
        var path = BuildPersistPath(context.Settings, context.TopicName);
        _logger.LogInformation("Persisting {Count} producer envelopes for {Topic} to {Path}", context.Envelopes.Count, context.TopicName, path);

        var payloads = new List<PersistedEnvelope>(context.Envelopes.Count);
        foreach (var envelope in context.Envelopes)
        {
            payloads.Add(Convert(envelope));
        }

        await using (var stream = CreateWriteStream(path, context.Settings.FileCompression))
        {
            await JsonSerializer.SerializeAsync(stream, payloads, _jsonOptions, cancellationToken);
        }

        UpdateTopicStatus(context.TopicName, context.Settings, payloads.Count, new FileInfo(path).Length, DateTimeOffset.UtcNow);
    }

    public async Task<IReadOnlyList<OutgoingEnvelope>> RestoreAsync(ProducerBacklogRestoreContext context, CancellationToken cancellationToken)
    {
        if (!context.Settings.ReplayEnabled)
        {
            return Array.Empty<OutgoingEnvelope>();
        }

        var path = ResolveExistingPath(context.Settings, context.TopicName);
        if (path is null)
        {
            return Array.Empty<OutgoingEnvelope>();
        }

        _logger.LogInformation("Restoring producer backlog for {Topic} from {Path}", context.TopicName, path);
        List<PersistedEnvelope> payloads;
        await using (var stream = CreateReadStream(path))
        {
            payloads = await JsonSerializer.DeserializeAsync<List<PersistedEnvelope>>(stream, _jsonOptions, cancellationToken)
                       ?? new List<PersistedEnvelope>();
        }

        var envelopes = new List<OutgoingEnvelope>(payloads.Count);
        foreach (var payload in payloads)
        {
            var type = Type.GetType(payload.PayloadType, throwOnError: false);
            if (type is null)
            {
                _logger.LogWarning("Unable to resolve payload type {Type}; skipping persisted envelope", payload.PayloadType);
                continue;
            }

            var message = JsonSerializer.Deserialize(payload.SerializedPayload, type, _jsonOptions);
            if (message is null)
            {
                _logger.LogWarning("Failed to deserialize payload of type {Type}; skipping envelope", payload.PayloadType);
                continue;
            }

            envelopes.Add(new OutgoingEnvelope(
                message,
                type,
                payload.Topic,
                payload.Key,
                payload.Headers,
                payload.LocalCreatedAt));
        }

        HandlePostRestoreFile(path, context.Settings);
        UpdateTopicStatus(context.TopicName, context.Settings, 0, 0, DateTimeOffset.UtcNow);

        return envelopes;
    }

    public ProducerBacklogTopicStatus? GetTopicStatus(string topicName)
    {
        _topicStatuses.TryGetValue(topicName, out var status);
        return status;
    }

    public IReadOnlyDictionary<string, ProducerBacklogTopicStatus> GetAllTopics()
    {
        return _topicStatuses;
    }

    private void UpdateTopicStatus(string topic, ProducerBacklogSettings settings, long persistedMessages, long persistedBytes, DateTimeOffset timestamp)
    {
        var status = new ProducerBacklogTopicStatus
        {
            PersistenceEnabled = settings.PersistenceEnabled,
            PersistedMessages = persistedMessages,
            PersistedBytes = persistedBytes,
            LastPersistedAt = persistedMessages > 0 ? timestamp : null,
            ArchiveOnRestore = settings.ArchiveOnRestore
        };
        _topicStatuses.AddOrUpdate(topic, status, (_, _) => status);
    }

    private static string BuildPersistPath(ProducerBacklogSettings settings, string topic)
    {
        var extension = settings.FileCompression?.ToLowerInvariant() switch
        {
            "gzip" => "json.gz",
            "brotli" => "json.br",
            _ => "json"
        };
        return BuildPathWithExtension(settings, topic, extension);
    }

    private static string? ResolveExistingPath(ProducerBacklogSettings settings, string topic)
    {
        var candidates = new[]
        {
            BuildPathWithExtension(settings, topic, "json"),
            BuildPathWithExtension(settings, topic, "json.gz"),
            BuildPathWithExtension(settings, topic, "json.br")
        };

        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return null;
    }

    private static string BuildPathWithExtension(ProducerBacklogSettings settings, string topic, string extension)
    {
        var safeTopic = topic.Replace('/', '_').Replace('\\', '_');
        return Path.Combine(settings.Directory, $"{safeTopic}.producer.backlog.{extension}");
    }

    private static Stream CreateWriteStream(string path, string? compression)
    {
        var fileStream = File.Create(path);
        return compression?.ToLowerInvariant() switch
        {
            "gzip" => new GZipStream(fileStream, CompressionLevel.Optimal, leaveOpen: false),
            "brotli" => new BrotliStream(fileStream, CompressionLevel.Optimal, leaveOpen: false),
            _ => fileStream
        };
    }

    private static Stream CreateReadStream(string path)
    {
        var fileStream = File.OpenRead(path);
        return Path.GetExtension(path).ToLowerInvariant() switch
        {
            ".gz" => new GZipStream(fileStream, CompressionMode.Decompress, leaveOpen: false),
            ".br" => new BrotliStream(fileStream, CompressionMode.Decompress, leaveOpen: false),
            _ => fileStream
        };
    }

    private void HandlePostRestoreFile(string path, ProducerBacklogSettings settings)
    {
        try
        {
            if (settings.ArchiveOnRestore)
            {
                var archiveName = $"{path}.archive-{DateTime.UtcNow:yyyyMMddHHmmssfff}";
                File.Move(path, archiveName, overwrite: true);
                _logger.LogInformation("Archived backlog file {Path} to {Archive}", path, archiveName);
            }
            else
            {
                File.Delete(path);
            }
        }
        catch (IOException ex)
        {
            _logger.LogWarning(ex, "Failed to finalize backlog file {Path}", path);
        }
    }

    private PersistedEnvelope Convert(OutgoingEnvelope envelope)
    {
        var json = JsonSerializer.Serialize(envelope.Payload, envelope.PayloadType, _jsonOptions);
        var headers = envelope.Headers is null ? null : new Dictionary<string, string>(envelope.Headers, StringComparer.OrdinalIgnoreCase);
        return new PersistedEnvelope
        {
            PayloadType = envelope.PayloadType.AssemblyQualifiedName ?? envelope.PayloadType.FullName!,
            SerializedPayload = json,
            Topic = envelope.Topic,
            Key = envelope.Key,
            Headers = headers,
            LocalCreatedAt = envelope.LocalCreatedAt
        };
    }

    private sealed class PersistedEnvelope
    {
        public string PayloadType { get; init; } = string.Empty;
        public string SerializedPayload { get; init; } = string.Empty;
        public string Topic { get; init; } = string.Empty;
        public string? Key { get; init; }
        public IDictionary<string, string>? Headers { get; init; }
        public DateTimeOffset LocalCreatedAt { get; init; }
    }
}
