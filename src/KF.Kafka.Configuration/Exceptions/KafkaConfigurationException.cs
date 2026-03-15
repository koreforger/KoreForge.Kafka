using System;
using System.Collections.Generic;

namespace KF.Kafka.Configuration.Exceptions;

public abstract class KafkaConfigurationException : Exception
{
    public string ProfileName { get; }

    protected KafkaConfigurationException(string profileName, string message)
        : base(message)
    {
        ProfileName = profileName;
    }
}

public sealed class KafkaProfileNotFoundException : KafkaConfigurationException
{
    public KafkaProfileNotFoundException(string profileName)
        : base(profileName, $"Kafka profile '{profileName}' was not found.")
    {
    }
}

public sealed class KafkaProfileValidationException : KafkaConfigurationException
{
    public IReadOnlyList<string> Errors { get; }

    public KafkaProfileValidationException(string profileName, IReadOnlyList<string> errors)
        : base(profileName, $"Kafka profile '{profileName}' is invalid: {string.Join("; ", errors)}")
    {
        Errors = errors;
    }
}
