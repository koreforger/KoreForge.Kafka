using System;
using System.Collections.Concurrent;
using System.IO;
using Confluent.Kafka;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Consumer.Logging;
using Microsoft.Extensions.Logging;

namespace KF.Kafka.Consumer.Execution;

internal sealed class CertificateWorkspace : IAsyncDisposable
{
    private readonly KafkaSecuritySettings _security;
    private readonly ExtendedConsumerSettings _settings;
    private readonly ConsumerConfig _baselineConfig;
    private readonly CertificatesLogger<CertificateWorkspace> _logs;
    private readonly string _rootDirectory;
    private readonly ConcurrentBag<string> _createdFiles = new();
    private readonly ConcurrentBag<string> _createdDirectories = new();

    public CertificateWorkspace(
        KafkaSecuritySettings security,
        ConsumerConfig baselineConfig,
        ExtendedConsumerSettings settings,
        ILogger<CertificateWorkspace> logger)
    {
        _security = security ?? new KafkaSecuritySettings();
        _baselineConfig = baselineConfig ?? throw new ArgumentNullException(nameof(baselineConfig));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logs = new CertificatesLogger<CertificateWorkspace>(logger ?? throw new ArgumentNullException(nameof(logger)));
        _rootDirectory = Path.Combine(Path.GetTempPath(), "khaos-kafka-consumer", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_rootDirectory);
        _logs.Isolation.Configured.LogInformation("Certificate workspace prepared at {Path}", _rootDirectory);
    }

    public void Apply(ConsumerConfig config, int workerIndex)
    {
        if (config == null) throw new ArgumentNullException(nameof(config));
        if (!_settings.IsolateConsumerCertificatePerThread || _security.Mode != KafkaSecurityMode.Certificates)
        {
            return;
        }

        var workerDirectory = Path.Combine(_rootDirectory, $"worker-{workerIndex + 1}");
        Directory.CreateDirectory(workerDirectory);
        _createdDirectories.Add(workerDirectory);

        config.SslCaLocation = Materialize(workerDirectory, "root-ca.pem", _security.RootCaBase64, _baselineConfig.SslCaLocation);
        config.SslCertificateLocation = Materialize(workerDirectory, "client-cert.pem", _security.ClientCertBase64, _baselineConfig.SslCertificateLocation);
        config.SslKeyLocation = Materialize(workerDirectory, "client-key.pem", _security.ClientKeyBase64, _baselineConfig.SslKeyLocation);

        if (!string.IsNullOrWhiteSpace(_security.ClientKeyPassword))
        {
            config.SslKeyPassword = _security.ClientKeyPassword;
        }

        _logs.Isolation.Created.LogDebug("Worker {WorkerId} certificate files isolated at {Directory}", workerIndex + 1, workerDirectory);
    }

    private string Materialize(string directory, string fileName, string? base64, string? sourcePath)
    {
        var destination = Path.Combine(directory, fileName);
        if (_settings.DuplicateCertificatePerWorker && !string.IsNullOrWhiteSpace(base64))
        {
            WriteFromBase64(destination, base64);
            return destination;
        }

        if (!string.IsNullOrWhiteSpace(sourcePath) && File.Exists(sourcePath))
        {
            File.Copy(sourcePath, destination, overwrite: true);
            File.SetAttributes(destination, FileAttributes.ReadOnly);
            _createdFiles.Add(destination);
            return destination;
        }

        if (!string.IsNullOrWhiteSpace(base64))
        {
            WriteFromBase64(destination, base64);
            return destination;
        }

        throw new InvalidOperationException("Kafka security profile is missing certificate data required for isolation.");
    }

    private void WriteFromBase64(string path, string base64)
    {
        var data = Convert.FromBase64String(base64);
        File.WriteAllBytes(path, data);
        File.SetAttributes(path, FileAttributes.ReadOnly);
        _createdFiles.Add(path);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var file in _createdFiles)
        {
            try
            {
                File.SetAttributes(file, FileAttributes.Normal);
                File.Delete(file);
            }
            catch (Exception ex)
            {
                _logs.Isolation.Cleanupfailed.LogWarning(ex, "Failed to delete isolated certificate file {File}", file);
            }
        }

        foreach (var directory in _createdDirectories)
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex)
            {
                _logs.Isolation.Cleanupfailed.LogWarning(ex, "Failed to delete certificate directory {Directory}", directory);
            }
        }

        try
        {
            if (Directory.Exists(_rootDirectory))
            {
                Directory.Delete(_rootDirectory, recursive: true);
            }
        }
        catch (Exception ex)
        {
            _logs.Isolation.Cleanupfailed.LogWarning(ex, "Failed to delete certificate workspace root {Path}", _rootDirectory);
        }

        await ValueTask.CompletedTask;
    }
}
