using System;
using Confluent.Kafka;
using KF.Kafka.Configuration.Options;

namespace KF.Kafka.Configuration.Security;

public interface IKafkaSecurityProvider
{
    void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security);
}

public sealed class DefaultKafkaSecurityProvider : IKafkaSecurityProvider
{
    private readonly IKafkaCertificateStore _certificateStore;

    public DefaultKafkaSecurityProvider(IKafkaCertificateStore certificateStore)
    {
        _certificateStore = certificateStore;
    }

    public void ApplySecurity(ClientConfig clientConfig, string profileName, KafkaSecuritySettings security)
    {
        if (clientConfig == null) throw new ArgumentNullException(nameof(clientConfig));
        if (security == null) throw new ArgumentNullException(nameof(security));

        if (security.Mode == KafkaSecurityMode.None)
        {
            return;
        }

        var files = _certificateStore.EnsureCertificateFiles(profileName, security);
        clientConfig.SecurityProtocol = SecurityProtocol.Ssl;
        clientConfig.SslCaLocation = files.RootCaPath;
        clientConfig.SslCertificateLocation = files.ClientCertificatePath;
        clientConfig.SslKeyLocation = files.ClientKeyPath;

        if (!string.IsNullOrEmpty(security.ClientKeyPassword))
        {
            clientConfig.SslKeyPassword = security.ClientKeyPassword;
        }
    }
}
