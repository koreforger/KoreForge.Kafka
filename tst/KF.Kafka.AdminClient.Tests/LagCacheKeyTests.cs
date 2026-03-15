using FluentAssertions;
using KF.Kafka.AdminClient.Internal;
using KF.Kafka.AdminClient.Models;
using Xunit;

namespace KF.Kafka.AdminClient.Tests;

public sealed class LagCacheKeyTests
{
    [Fact]
    public void Uses_wildcard_when_topic_filter_is_empty()
    {
        var key = LagCacheKey.From("group", new ConsumerGroupLagQuery());

        key.ConsumerGroupId.Should().Be("group");
        key.TopicsKey.Should().Be("*");
        key.ExcludeEmpty.Should().BeFalse();
    }

    [Fact]
    public void Normalizes_topics_by_trimming_and_sorting_case_insensitively()
    {
        var query = new ConsumerGroupLagQuery
        {
            TopicFilter = new[] { " payments ", "EVENTS", "payments" },
            ExcludeEmptyTopics = true
        };

        var key = LagCacheKey.From("group", query);
        key.TopicsKey.Should().Be("events|payments|payments");
        key.ExcludeEmpty.Should().BeTrue();
    }
}
