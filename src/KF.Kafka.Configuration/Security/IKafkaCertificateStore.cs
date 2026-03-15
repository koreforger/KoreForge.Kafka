using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using KF.Kafka.Configuration.Options;
using Microsoft.Extensions.Logging;

namespace KF.Kafka.Configuration.Security;

public sealed class KafkaCertificateFileSet
{
    public string RootCaPath { get; }
    public string ClientCertificatePath { get; }
    public string ClientKeyPath { get; }

    public KafkaCertificateFileSet(string rootCaPath, string clientCertificatePath, string clientKeyPath)
    {
        RootCaPath = rootCaPath;
        ClientCertificatePath = clientCertificatePath;
        ClientKeyPath = clientKeyPath;
    }
}

public interface IKafkaCertificateStore : IAsyncDisposable
{
    KafkaCertificateFileSet EnsureCertificateFiles(string profileName, KafkaSecuritySettings security);
}

public sealed class FileKafkaCertificateStore : IKafkaCertificateStore
{
    private readonly ConcurrentDictionary<string, Lazy<KafkaCertificateFileSet>> _cache = new();
    private readonly ILogger<FileKafkaCertificateStore> _logger;
    private readonly ConcurrentBag<string> _createdFiles = new();
    private readonly string _rootDirectory;

    public FileKafkaCertificateStore(ILogger<FileKafkaCertificateStore> logger)
    {
        _logger = logger;
        _rootDirectory = Path.Combine(Path.GetTempPath(), "khaos-kafka-certs");
        Directory.CreateDirectory(_rootDirectory);
    }

    public KafkaCertificateFileSet EnsureCertificateFiles(string profileName, KafkaSecuritySettings security)
    {
        if (security == null) throw new ArgumentNullException(nameof(security));

        if (security.Mode != KafkaSecurityMode.Certificates)
        {
            throw new InvalidOperationException("Certificate files are only required for certificate-based security profiles.");
        }

        var lazy = _cache.GetOrAdd(profileName, _ =>
            new Lazy<KafkaCertificateFileSet>(() => CreateFiles(profileName, security), LazyThreadSafetyMode.ExecutionAndPublication));

        return lazy.Value;
    }

    private KafkaCertificateFileSet CreateFiles(string profileName, KafkaSecuritySettings security)
    {
        var id = Guid.NewGuid().ToString("N");
        var rootPath = WriteCertificateFile(profileName, $"{id}_R.pem", security.RootCaBase64, "Root CA");
        var certPath = WriteCertificateFile(profileName, $"{id}_C.pem", security.ClientCertBase64, "Client certificate");
        var keyPath = WriteCertificateFile(profileName, $"{id}_K.pem", security.ClientKeyBase64, "Client key");

        _logger.LogInformation("Kafka security profile {Profile} stored certificates at {Root}, {Cert}, {Key}", profileName, rootPath, certPath, keyPath);

        return new KafkaCertificateFileSet(rootPath, certPath, keyPath);
    }

    private string WriteCertificateFile(string profileName, string fileName, string? base64, string description)
    {
        if (string.IsNullOrWhiteSpace(base64))
        {
            throw new InvalidOperationException($"Security profile '{profileName}' is missing {description} data.");
        }

        var path = Path.Combine(_rootDirectory, fileName);
        var data = Convert.FromBase64String(base64);
        File.WriteAllBytes(path, data);
        File.SetAttributes(path, FileAttributes.ReadOnly);
        _createdFiles.Add(path);
        return path;
    }

    public ValueTask DisposeAsync()
    {
        foreach (var file in _createdFiles)
        {
            try
            {
                File.SetAttributes(file, FileAttributes.Normal);
                File.Delete(file);
            }
            catch
            {
                // Intentionally ignored to avoid throwing during disposal.
            }
        }

        return ValueTask.CompletedTask;
    }
}
