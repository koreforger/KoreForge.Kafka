using System.Text;
using KoreForge.Kafka.AdminClient.Models;

namespace KoreForge.Kafka.AdminClient.Internal;

internal readonly record struct LagCacheKey(string ConsumerGroupId, string TopicsKey, bool ExcludeEmpty)
{
    public static LagCacheKey From(string consumerGroupId, ConsumerGroupLagQuery query)
    {
        var normalizedTopics = NormalizeTopics(query.TopicFilter);
        return new LagCacheKey(consumerGroupId, normalizedTopics, query.ExcludeEmptyTopics);
    }

    private static string NormalizeTopics(IReadOnlyList<string>? topics)
    {
        if (topics is null || topics.Count == 0)
        {
            return "*";
        }

        var ordered = topics
            .Where(static t => !string.IsNullOrWhiteSpace(t))
            .Select(static t => t.Trim())
            .OrderBy(static t => t, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var builder = new StringBuilder();
        for (var i = 0; i < ordered.Length; i++)
        {
            if (i > 0)
            {
                builder.Append('|');
            }

            builder.Append(ordered[i].ToLowerInvariant());
        }

        return builder.ToString();
    }
}
