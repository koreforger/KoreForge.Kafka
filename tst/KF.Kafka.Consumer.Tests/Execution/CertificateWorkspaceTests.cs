using System.Text;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Consumer.Execution;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KF.Kafka.Consumer.Tests.Execution;

public sealed class CertificateWorkspaceTests
{
    [Fact]
    public async Task Apply_WithDuplicatedCertificates_WritesIsolatedFiles()
    {
        var security = new KafkaSecuritySettings
        {
            Mode = KafkaSecurityMode.Certificates,
            RootCaBase64 = ToBase64("root-ca"),
            ClientCertBase64 = ToBase64("client-cert"),
            ClientKeyBase64 = ToBase64("client-key"),
            ClientKeyPassword = "secret"
        };

        var extended = new ExtendedConsumerSettings
        {
            IsolateConsumerCertificatePerThread = true,
            DuplicateCertificatePerWorker = true
        };

        var targetConfig = new ConsumerConfig();
        await using var workspace = new CertificateWorkspace(
            security,
            new ConsumerConfig(),
            extended,
            NullLogger<CertificateWorkspace>.Instance);

        workspace.Apply(targetConfig, workerIndex: 0);

        targetConfig.SslCaLocation.Should().NotBeNullOrWhiteSpace();
        File.Exists(targetConfig.SslCaLocation!).Should().BeTrue();
        File.ReadAllText(targetConfig.SslCaLocation!).Should().Be("root-ca");
        targetConfig.SslCertificateLocation.Should().NotBeNull();
        File.ReadAllText(targetConfig.SslCertificateLocation!).Should().Be("client-cert");
        targetConfig.SslKeyLocation.Should().NotBeNull();
        File.ReadAllText(targetConfig.SslKeyLocation!).Should().Be("client-key");
        targetConfig.SslKeyPassword.Should().Be("secret");

        await workspace.DisposeAsync();

        File.Exists(targetConfig.SslCaLocation!).Should().BeFalse();
        File.Exists(targetConfig.SslCertificateLocation!).Should().BeFalse();
        File.Exists(targetConfig.SslKeyLocation!).Should().BeFalse();
    }

    [Fact]
    public async Task Apply_WithSharedCertificates_CopiesExistingFiles()
    {
        var caSource = CreateTempFile("shared-ca");
        var certSource = CreateTempFile("shared-cert");
        var keySource = CreateTempFile("shared-key");

        var baseline = new ConsumerConfig
        {
            SslCaLocation = caSource,
            SslCertificateLocation = certSource,
            SslKeyLocation = keySource
        };

        var security = new KafkaSecuritySettings
        {
            Mode = KafkaSecurityMode.Certificates
        };

        var extended = new ExtendedConsumerSettings
        {
            IsolateConsumerCertificatePerThread = true,
            DuplicateCertificatePerWorker = false
        };

        var targetConfig = new ConsumerConfig();
        await using var workspace = new CertificateWorkspace(
            security,
            baseline,
            extended,
            NullLogger<CertificateWorkspace>.Instance);

        workspace.Apply(targetConfig, workerIndex: 1);

        targetConfig.SslCaLocation.Should().NotBeNull();
        targetConfig.SslCaLocation.Should().NotBe(caSource);
        File.ReadAllText(targetConfig.SslCaLocation!).Should().Be("shared-ca");

        targetConfig.SslCertificateLocation.Should().NotBe(certSource);
        File.ReadAllText(targetConfig.SslCertificateLocation!).Should().Be("shared-cert");

        targetConfig.SslKeyLocation.Should().NotBe(keySource);
        File.ReadAllText(targetConfig.SslKeyLocation!).Should().Be("shared-key");

        await workspace.DisposeAsync();

        File.Exists(targetConfig.SslCaLocation!).Should().BeFalse();
        File.Exists(targetConfig.SslCertificateLocation!).Should().BeFalse();
        File.Exists(targetConfig.SslKeyLocation!).Should().BeFalse();

        File.Exists(caSource).Should().BeTrue();
        File.Exists(certSource).Should().BeTrue();
        File.Exists(keySource).Should().BeTrue();

        File.Delete(caSource);
        File.Delete(certSource);
        File.Delete(keySource);
    }

    private static string ToBase64(string content) => Convert.ToBase64String(Encoding.UTF8.GetBytes(content));

    private static string CreateTempFile(string content)
    {
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".pem");
        File.WriteAllText(path, content);
        return path;
    }
}
