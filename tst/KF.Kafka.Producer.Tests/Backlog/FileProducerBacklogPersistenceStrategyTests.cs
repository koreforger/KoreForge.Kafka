using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using KF.Kafka.Configuration.Producer;
using KF.Kafka.Producer.Abstractions;
using KF.Kafka.Producer.Backlog;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KF.Kafka.Producer.Tests.Backlog;

public sealed class FileProducerBacklogPersistenceStrategyTests
{
    [Fact]
    public async Task PersistAndRestore_RoundTripsEnvelopes()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        try
        {
            var settings = new ProducerBacklogSettings
            {
                PersistenceEnabled = true,
                ReplayEnabled = true,
                Directory = directory
            };
            var strategy = new FileProducerBacklogPersistenceStrategy(NullLogger<FileProducerBacklogPersistenceStrategy>.Instance);
            var envelopes = new List<OutgoingEnvelope>
            {
                new("hello", typeof(string), "topic-a", null, null, DateTimeOffset.Parse("2024-01-01T00:00:00Z")),
                new(42, typeof(int), "topic-a", "k", new Dictionary<string, string> { ["trace"] = "1" }, DateTimeOffset.Parse("2024-01-02T00:00:00Z"))
            };

            var persistContext = new ProducerBacklogPersistContext("topic-a", envelopes, settings);
            await strategy.PersistAsync(persistContext, CancellationToken.None);

            var restoreContext = new ProducerBacklogRestoreContext("topic-a", settings);
            var restored = await strategy.RestoreAsync(restoreContext, CancellationToken.None);

            restored.Should().HaveCount(2);
            restored[0].Payload.Should().Be("hello");
            restored[1].Payload.Should().Be(42);
            restored[1].Headers.Should().ContainKey("trace");
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Persist_WritesCompressedPayload_WhenCompressionEnabled()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        try
        {
            var settings = new ProducerBacklogSettings
            {
                PersistenceEnabled = true,
                Directory = directory,
                FileCompression = "gzip"
            };
            var strategy = new FileProducerBacklogPersistenceStrategy(NullLogger<FileProducerBacklogPersistenceStrategy>.Instance);
            var envelopes = new List<OutgoingEnvelope> { new("payload", typeof(string), "topic-a", null, null, DateTimeOffset.UtcNow) };

            var persistContext = new ProducerBacklogPersistContext("topic-a", envelopes, settings);
            await strategy.PersistAsync(persistContext, CancellationToken.None);

            var file = Directory.EnumerateFiles(directory).Single();
            file.Should().EndWith("json.gz");
            new FileInfo(file).Length.Should().BeGreaterThan(0);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Restore_ArchivesBacklog_WhenConfigured()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        try
        {
            var settings = new ProducerBacklogSettings
            {
                PersistenceEnabled = true,
                ReplayEnabled = true,
                Directory = directory,
                ArchiveOnRestore = true
            };
            var strategy = new FileProducerBacklogPersistenceStrategy(NullLogger<FileProducerBacklogPersistenceStrategy>.Instance);
            var envelopes = new List<OutgoingEnvelope> { new("payload", typeof(string), "topic-a", null, null, DateTimeOffset.UtcNow) };
            await strategy.PersistAsync(new ProducerBacklogPersistContext("topic-a", envelopes, settings), CancellationToken.None);

            var restoreContext = new ProducerBacklogRestoreContext("topic-a", settings);
            var restored = await strategy.RestoreAsync(restoreContext, CancellationToken.None);
            restored.Should().HaveCount(1);

            Directory.EnumerateFiles(directory).Should().ContainSingle(f => f.Contains("archive"));
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Diagnostics_TracksPersistedCounts()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
        try
        {
            var settings = new ProducerBacklogSettings
            {
                PersistenceEnabled = true,
                ReplayEnabled = true,
                Directory = directory
            };
            var strategy = new FileProducerBacklogPersistenceStrategy(NullLogger<FileProducerBacklogPersistenceStrategy>.Instance);
            var diagnostics = (IProducerBacklogDiagnostics)strategy;

            var envelopes = new List<OutgoingEnvelope> { new("payload", typeof(string), "topic-a", null, null, DateTimeOffset.UtcNow) };
            await strategy.PersistAsync(new ProducerBacklogPersistContext("topic-a", envelopes, settings), CancellationToken.None);

            var status = diagnostics.GetTopicStatus("topic-a");
            status.Should().NotBeNull();
            status!.PersistedMessages.Should().Be(1);
            status.PersistedBytes.Should().BeGreaterThan(0);
            status.LastPersistedAt.Should().NotBeNull();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }
}
