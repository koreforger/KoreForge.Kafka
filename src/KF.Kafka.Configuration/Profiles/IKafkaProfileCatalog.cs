using System;
using System.Collections.Generic;
using System.Linq;
using KF.Kafka.Configuration.Exceptions;
using KF.Kafka.Configuration.Options;
using Microsoft.Extensions.Options;

namespace KF.Kafka.Configuration.Profiles;

public interface IKafkaProfileCatalog
{
    IReadOnlyCollection<string> GetProfileNames();
    KafkaProfileSettings GetProfile(string profileName);
}

public sealed class KafkaProfileCatalog : IKafkaProfileCatalog
{
    private readonly IOptionsMonitor<KafkaConfigurationRootOptions> _options;

    public KafkaProfileCatalog(IOptionsMonitor<KafkaConfigurationRootOptions> options)
    {
        _options = options;
    }

    public IReadOnlyCollection<string> GetProfileNames()
    {
        return _options.CurrentValue.Profiles.Keys.ToArray();
    }

    public KafkaProfileSettings GetProfile(string profileName)
    {
        if (!_options.CurrentValue.Profiles.TryGetValue(profileName, out var profile))
        {
            throw new KafkaProfileNotFoundException(profileName);
        }

        return profile;
    }
}
