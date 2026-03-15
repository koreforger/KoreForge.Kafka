using System.Text;
using KF.Kafka.Configuration.Admin;
using Microsoft.Extensions.Options;

namespace KF.Kafka.AdminClient.Internal;

internal sealed class KafkaAdminOptionsValidator : IValidateOptions<KafkaAdminOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaAdminOptions options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            errors.Add("BootstrapServers is required.");
        }

        if (options.RequestTimeoutMs <= 0)
        {
            errors.Add("RequestTimeoutMs must be greater than zero.");
        }

        if (options.MaxDegreeOfParallelism <= 0)
        {
            errors.Add("MaxDegreeOfParallelism must be at least 1.");
        }

        if (options.Retry.InitialBackoffMs <= 0)
        {
            errors.Add("Retry.InitialBackoffMs must be greater than zero.");
        }

        if (options.Retry.MaxBackoffMs < options.Retry.InitialBackoffMs)
        {
            errors.Add("Retry.MaxBackoffMs must be greater than or equal to InitialBackoffMs.");
        }

        if (errors.Count == 0)
        {
            return ValidateOptionsResult.Success;
        }

        var message = new StringBuilder()
            .Append("KafkaAdminOptions validation failed: ")
            .Append(string.Join("; ", errors))
            .ToString();

        return ValidateOptionsResult.Fail(message);
    }
}
