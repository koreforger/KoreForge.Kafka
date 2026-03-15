using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace KF.Kafka.Configuration.Generation;

public interface IKafkaGroupIdGenerator
{
    string GenerateGroupId(string template, string profileName);
}

public sealed class DefaultKafkaGroupIdGenerator : IKafkaGroupIdGenerator
{
    private static readonly string ApplicationName = AppDomain.CurrentDomain.FriendlyName;
    private static readonly string EnvironmentName =
        Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ??
        Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ??
        "Unknown";

    public string GenerateGroupId(string template, string profileName)
    {
        if (string.IsNullOrWhiteSpace(template))
        {
            throw new ArgumentException("Template cannot be null or empty.", nameof(template));
        }

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            {"AppName", ApplicationName},
            {"Environment", EnvironmentName},
            {"MachineName", Environment.MachineName},
            {"UserName", Environment.UserName},
            {"ProfileName", profileName},
            {"Guid", Guid.NewGuid().ToString()},
            {"Random4", GenerateRandomToken()}
        };

        var builder = new StringBuilder(template);
        foreach (var pair in map)
        {
            builder.Replace($"{{{pair.Key}}}", pair.Value);
        }

        return builder.ToString();
    }

    private static string GenerateRandomToken()
    {
        Span<byte> buffer = stackalloc byte[2];
        RandomNumberGenerator.Fill(buffer);
        return BitConverter.ToString(buffer.ToArray()).Replace("-", string.Empty);
    }
}
