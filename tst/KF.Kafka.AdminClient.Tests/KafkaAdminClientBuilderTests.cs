using System.Reflection;
using Confluent.Kafka;
using FluentAssertions;
using KF.Kafka.AdminClient.Builder;
using KF.Kafka.Configuration.Admin;
using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace KF.Kafka.AdminClient.Tests;

public sealed class KafkaAdminClientBuilderTests
{
    [Fact]
    public void UseKafkaConfigurationProfile_AppliesBootstrapAndSecurity()
    {
        var runtime = new KafkaAdminRuntimeConfig(
            new AdminClientConfig
            {
                BootstrapServers = "config-host:9093",
                ClientId = "config-client",
                SecurityProtocol = SecurityProtocol.Ssl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "user",
                SaslPassword = "pass",
                SslCaLocation = "C:/certs/root.pem",
                SocketTimeoutMs = 5000
            },
            new ExtendedAdminSettings());

        var factory = Substitute.For<IKafkaClientConfigFactory>();
        factory.CreateAdmin("admin-profile").Returns(runtime);

        var builder = new KafkaAdminClientBuilder()
            .UseLoggerFactory(NullLoggerFactory.Instance)
            .UseKafkaConfigurationProfile("admin-profile", factory);

        var options = (KafkaAdminOptions)typeof(KafkaAdminClientBuilder)
            .GetField("_options", BindingFlags.NonPublic | BindingFlags.Instance)!.
            GetValue(builder)!;

        options.BootstrapServers.Should().Be("config-host:9093");
        options.ClientId.Should().Be("config-client");
        options.RequestTimeoutMs.Should().Be(5000);
        options.Security.SecurityProtocol.Should().Be("Ssl");
        options.Security.SaslMechanism.Should().Be("Plain");
        options.Security.SaslUsername.Should().Be("user");
        options.Security.SaslPassword.Should().Be("pass");
        options.Security.SslCaLocation.Should().Be("C:/certs/root.pem");

        var adminConfig = (AdminClientConfig?)typeof(KafkaAdminClientBuilder)
            .GetField("_adminClientConfig", BindingFlags.NonPublic | BindingFlags.Instance)!.
            GetValue(builder);

        adminConfig.Should().NotBeNull();
        adminConfig!.BootstrapServers.Should().Be("config-host:9093");

        factory.Received(1).CreateAdmin("admin-profile");
    }
}
