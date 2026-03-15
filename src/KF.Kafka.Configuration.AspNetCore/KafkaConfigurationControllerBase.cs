using System;
using System.Collections.Generic;
using System.Linq;
using KF.Kafka.Configuration.Factory;
using KF.Kafka.Configuration.Logging;
using KF.Kafka.Configuration.Options;
using KF.Kafka.Configuration.Profiles;
using KF.Kafka.Configuration.Runtime;
using KF.Kafka.Configuration.Validation;
using Microsoft.AspNetCore.Mvc;

namespace KF.Kafka.Configuration.AspNetCore;

public abstract class KafkaConfigurationControllerBase : ControllerBase
{
    protected IKafkaProfileCatalog Profiles { get; }
    protected IKafkaClientConfigFactory Factory { get; }
    protected IKafkaConfigValidator Validator { get; }
    protected IKafkaConfigLogger ConfigLogger { get; }

    protected KafkaConfigurationControllerBase(
        IKafkaProfileCatalog profiles,
        IKafkaClientConfigFactory factory,
        IKafkaConfigValidator validator,
        IKafkaConfigLogger configLogger)
    {
        Profiles = profiles;
        Factory = factory;
        Validator = validator;
        ConfigLogger = configLogger;
    }

    protected sealed record KafkaProfileSummaryDto(
        string Name,
        KafkaClientType Type,
        string Cluster,
        string? SecurityProfile);

    protected sealed record KafkaProfileDetailDto(
        string Name,
        KafkaClientType Type,
        string Cluster,
        string? SecurityProfile,
        IReadOnlyCollection<string> Topics,
        bool IsValid,
        IReadOnlyList<string> Errors,
        IReadOnlyList<string> Warnings,
        string ConfigString,
        IReadOnlyDictionary<string, object?> ConfigProperties,
        ExtendedConsumerSettings? ExtendedConsumer,
        ExtendedProducerSettings? ExtendedProducer,
        ExtendedAdminSettings? ExtendedAdmin);

    protected virtual ActionResult<IEnumerable<KafkaProfileSummaryDto>> GetProfilesInternal()
    {
        var summaries = Profiles
            .GetProfileNames()
            .Select(name =>
            {
                var profile = Profiles.GetProfile(name);
                return new KafkaProfileSummaryDto(name, profile.Type, profile.Cluster, profile.SecurityProfile);
            })
            .ToArray();

        return Ok(summaries);
    }

    protected virtual ActionResult<KafkaProfileDetailDto> GetProfileDetailInternal(string profileName)
    {
        var profile = Profiles.GetProfile(profileName);

        string configString;
        IReadOnlyDictionary<string, object?> logProps;
        IReadOnlyCollection<string> topics;
        ExtendedConsumerSettings? consumer = null;
        ExtendedProducerSettings? producer = null;
        ExtendedAdminSettings? admin = null;
        KafkaConfigValidationResult validation;

        switch (profile.Type)
        {
            case KafkaClientType.Consumer:
                var consumerCfg = Factory.CreateConsumer(profileName);
                validation = Validator.ValidateConsumer(consumerCfg);
                configString = ConfigLogger.ToLogString(consumerCfg.ConfluentConfig);
                logProps = ConfigLogger.ToLogProperties(consumerCfg.ConfluentConfig);
                topics = consumerCfg.Topics;
                consumer = consumerCfg.Extended;
                break;

            case KafkaClientType.Producer:
                var producerCfg = Factory.CreateProducer(profileName);
                validation = Validator.ValidateProducer(producerCfg);
                configString = ConfigLogger.ToLogString(producerCfg.ConfluentConfig);
                logProps = ConfigLogger.ToLogProperties(producerCfg.ConfluentConfig);
                topics = Array.Empty<string>();
                producer = producerCfg.Extended;
                break;

            case KafkaClientType.Admin:
                var adminCfg = Factory.CreateAdmin(profileName);
                validation = Validator.ValidateAdmin(adminCfg);
                configString = ConfigLogger.ToLogString(adminCfg.ConfluentConfig);
                logProps = ConfigLogger.ToLogProperties(adminCfg.ConfluentConfig);
                topics = Array.Empty<string>();
                admin = adminCfg.Extended;
                break;

            default:
                throw new ArgumentOutOfRangeException();
        }

        var dto = new KafkaProfileDetailDto(
            profileName,
            profile.Type,
            profile.Cluster,
            profile.SecurityProfile,
            topics,
            validation.IsValid,
            validation.Errors,
            validation.Warnings,
            configString,
            logProps,
            consumer,
            producer,
            admin);

        return Ok(dto);
    }
}
