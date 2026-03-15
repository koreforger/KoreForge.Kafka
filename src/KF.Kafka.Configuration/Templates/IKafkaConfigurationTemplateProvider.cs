using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using KF.Kafka.Configuration.Options;

namespace KF.Kafka.Configuration.Templates;

public interface IKafkaConfigurationTemplateProvider
{
    string GenerateKafkaJsonTemplate();
}

public sealed class KafkaConfigurationTemplateProvider : IKafkaConfigurationTemplateProvider
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public string GenerateKafkaJsonTemplate()
    {
        var root = new KafkaConfigurationRootOptions
        {
            Defaults = new KafkaDefaultsOptions
            {
                GroupIdTemplate = "prod-{AppName}-{ProfileName}-{Random4}"
            }
        };

        root.Clusters["Primary"] = new KafkaClusterSettings
        {
            BootstrapServers = "pkafka01:9093,pkafka02:9093",
            ConfluentOptions =
            {
                ["security.protocol"] = "SSL"
            },
            DefaultSecurityProfile = "Internal"
        };

        root.SecurityProfiles["Internal"] = new KafkaSecuritySettings
        {
            Mode = KafkaSecurityMode.Certificates,
            RootCaBase64 = "BASE64-CA",
            ClientCertBase64 = "BASE64-CERT",
            ClientKeyBase64 = "BASE64-KEY"
        };

        root.SecurityProfiles["None"] = new KafkaSecuritySettings
        {
            Mode = KafkaSecurityMode.None
        };

        root.Profiles["PaymentsConsumer"] = new KafkaProfileSettings
        {
            Type = KafkaClientType.Consumer,
            Cluster = "Primary",
            SecurityProfile = "Internal",
            Topics =
            {
                "payments",
                "payments-dlq"
            },
            ConfluentOptions =
            {
                ["auto.offset.reset"] = "earliest",
                ["enable.auto.commit"] = "false"
            },
            ExtendedConsumer = new ExtendedConsumerSettings
            {
                ConsumerCount = 4,
                StartMode = StartMode.RelativeTime,
                StartRelative = TimeSpan.FromHours(1),
                MaxBatchSize = 750,
                MaxBatchWaitMs = 500,
                IsolateConsumerCertificatePerThread = true,
                DuplicateCertificatePerWorker = false,
                Backpressure = new ConsumerBackpressureSettings
                {
                    Coordinated = true,
                    PauseUtilization = 0.9,
                    ResumeUtilization = 0.4
                }
            }
        };

        root.Profiles["AuditProducer"] = new KafkaProfileSettings
        {
            Type = KafkaClientType.Producer,
            Cluster = "Primary",
            SecurityProfile = "None",
            ConfluentOptions =
            {
                ["compression.type"] = "lz4",
                ["linger.ms"] = "5"
            }
        };

        var payload = new { Kafka = root };
        return JsonSerializer.Serialize(payload, SerializerOptions);
    }
}
