using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Security;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace KoreForge.Kafka.Configuration.Tests;

public class DefaultKafkaSecurityProviderTests
{
    [Fact]
    public async Task ApplySecurity_WritesCertificates_WhenModeIsCertificates()
    {
        await using var store = new FileKafkaCertificateStore(NullLogger<FileKafkaCertificateStore>.Instance);
        var provider = new DefaultKafkaSecurityProvider(store);
        var security = new KafkaSecuritySettings
        {
            Mode = KafkaSecurityMode.Certificates,
            RootCaBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes("root")),
            ClientCertBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes("cert")),
            ClientKeyBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes("key")),
            ClientKeyPassword = "p@ss"
        };
        var config = new ClientConfig();

        provider.ApplySecurity(config, "profile", security);

        config.SecurityProtocol.Should().Be(SecurityProtocol.Ssl);
        config.SslKeyPassword.Should().Be("p@ss");
        config.SslCaLocation.Should().NotBeNullOrEmpty();
        config.SslCertificateLocation.Should().NotBeNullOrEmpty();
        config.SslKeyLocation.Should().NotBeNullOrEmpty();
    }
}
