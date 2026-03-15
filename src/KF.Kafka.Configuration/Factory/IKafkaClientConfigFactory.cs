using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using KF.Kafka.Configuration.Exceptions;
using KF.Kafka.Configuration.Generation;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Configuration.Security;
using KF.Kafka.Configuration.Validation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KF.Kafka.Configuration.Factory;

public interface IKafkaClientConfigFactory
{
    KafkaConsumerRuntimeConfig CreateConsumer(string profileName);
    KafkaProducerRuntimeConfig CreateProducer(string profileName);
    KafkaAdminRuntimeConfig CreateAdmin(string profileName);
}

public sealed class KafkaClientConfigFactory : IKafkaClientConfigFactory
{
    private readonly IOptionsMonitor<KafkaConfigurationRootOptions> _options;
    private readonly IKafkaGroupIdGenerator _groupIdGenerator;
    private readonly IKafkaSecurityProvider _securityProvider;
    private readonly IKafkaConfigValidator _validator;
    private readonly ILogger<KafkaClientConfigFactory> _logger;

    public KafkaClientConfigFactory(
        IOptionsMonitor<KafkaConfigurationRootOptions> options,
        IKafkaGroupIdGenerator groupIdGenerator,
        IKafkaSecurityProvider securityProvider,
        IKafkaConfigValidator validator,
        ILogger<KafkaClientConfigFactory> logger)
    {
        _options = options;
        _groupIdGenerator = groupIdGenerator;
        _securityProvider = securityProvider;
        _validator = validator;
        _logger = logger;
    }

    public KafkaConsumerRuntimeConfig CreateConsumer(string profileName)
    {
        var profile = GetProfile(profileName, KafkaClientType.Consumer);
        var cluster = GetCluster(profileName, profile);
        var security = ResolveSecurity(profileName, profile, cluster);

        var consumerConfig = BuildConfig<ConsumerConfig>(profile, cluster);
        if (!string.IsNullOrWhiteSpace(profile.ExplicitGroupId))
        {
            consumerConfig.GroupId = profile.ExplicitGroupId;
        }
        else if (string.IsNullOrWhiteSpace(consumerConfig.GroupId))
        {
            var template = profile.GroupIdTemplate ?? _options.CurrentValue.Defaults.GroupIdTemplate;
            consumerConfig.GroupId = _groupIdGenerator.GenerateGroupId(template, profileName);
        }

        _securityProvider.ApplySecurity(consumerConfig, profileName, security);

        var topicSource = profile.Topics ?? new List<string>();
        var topics = topicSource.Count == 0 ? Array.Empty<string>() : topicSource.ToArray();
        var runtime = new KafkaConsumerRuntimeConfig(
            consumerConfig,
            profile.ExtendedConsumer,
            topics,
            security);

        EnsureValid(profileName, () => _validator.ValidateConsumer(runtime));
        return runtime;
    }

    public KafkaProducerRuntimeConfig CreateProducer(string profileName)
    {
        var profile = GetProfile(profileName, KafkaClientType.Producer);
        var cluster = GetCluster(profileName, profile);
        var security = ResolveSecurity(profileName, profile, cluster);

        var producerConfig = BuildConfig<ProducerConfig>(profile, cluster);
        _securityProvider.ApplySecurity(producerConfig, profileName, security);

        var topicSource = profile.Topics ?? new List<string>();
        var topics = topicSource.Count == 0 ? Array.Empty<string>() : topicSource.ToArray();
        var runtime = new KafkaProducerRuntimeConfig(producerConfig, profile.ExtendedProducer, topics);
        EnsureValid(profileName, () => _validator.ValidateProducer(runtime));
        return runtime;
    }

    public KafkaAdminRuntimeConfig CreateAdmin(string profileName)
    {
        var profile = GetProfile(profileName, KafkaClientType.Admin);
        var cluster = GetCluster(profileName, profile);
        var security = ResolveSecurity(profileName, profile, cluster);

        var adminConfig = BuildConfig<AdminClientConfig>(profile, cluster);
        _securityProvider.ApplySecurity(adminConfig, profileName, security);

        var runtime = new KafkaAdminRuntimeConfig(adminConfig, profile.ExtendedAdmin);
        EnsureValid(profileName, () => _validator.ValidateAdmin(runtime));
        return runtime;
    }

    private KafkaProfileSettings GetProfile(string profileName, KafkaClientType expectedType)
    {
        if (!_options.CurrentValue.Profiles.TryGetValue(profileName, out var profile))
        {
            throw new KafkaProfileNotFoundException(profileName);
        }

        if (profile.Type != expectedType)
        {
            throw new KafkaProfileValidationException(profileName, new[]
            {
                $"Profile '{profileName}' is of type {profile.Type} but {expectedType} was requested."
            });
        }

        return profile;
    }

    private KafkaClusterSettings GetCluster(string profileName, KafkaProfileSettings profile)
    {
        if (!_options.CurrentValue.Clusters.TryGetValue(profile.Cluster, out var cluster))
        {
            throw new KafkaProfileValidationException(profileName, new[]
            {
                $"Cluster '{profile.Cluster}' referenced by profile '{profileName}' was not found."
            });
        }

        return cluster;
    }

    private KafkaSecuritySettings ResolveSecurity(string profileName, KafkaProfileSettings profile, KafkaClusterSettings cluster)
    {
        if (profile.SecurityProfile is not null)
        {
            if (_options.CurrentValue.SecurityProfiles.TryGetValue(profile.SecurityProfile, out var explicitSecurity))
            {
                return explicitSecurity;
            }

            throw new KafkaProfileValidationException(profileName, new[]
            {
                $"Security profile '{profile.SecurityProfile}' was not found."
            });
        }

        if (cluster.DefaultSecurityProfile is not null)
        {
            if (_options.CurrentValue.SecurityProfiles.TryGetValue(cluster.DefaultSecurityProfile, out var defaultSecurity))
            {
                return defaultSecurity;
            }

            throw new KafkaProfileValidationException(profileName, new[]
            {
                $"Default security profile '{cluster.DefaultSecurityProfile}' for cluster '{cluster.BootstrapServers}' was not found."
            });
        }

        return new KafkaSecuritySettings();
    }

    private static TConfig BuildConfig<TConfig>(KafkaProfileSettings profile, KafkaClusterSettings cluster)
        where TConfig : ClientConfig, new()
    {
        var merged = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in cluster.ConfluentOptions)
        {
            merged[kvp.Key] = kvp.Value;
        }

        foreach (var kvp in profile.ConfluentOptions)
        {
            merged[kvp.Key] = kvp.Value;
        }

        var config = new TConfig();
        foreach (var kvp in merged)
        {
            config.Set(kvp.Key, kvp.Value);
        }

        if (string.IsNullOrWhiteSpace(config.BootstrapServers))
        {
            config.BootstrapServers = cluster.BootstrapServers;
        }

        return config;
    }

    private void EnsureValid(string profileName, Func<KafkaConfigValidationResult> validator)
    {
        var result = validator();
        if (result.IsValid)
        {
            if (result.Warnings.Count > 0)
            {
                _logger.LogWarning("Kafka profile {Profile} validation warnings: {Warnings}", profileName, string.Join(", ", result.Warnings));
            }
            return;
        }

        throw new KafkaProfileValidationException(profileName, result.Errors);
    }
}
