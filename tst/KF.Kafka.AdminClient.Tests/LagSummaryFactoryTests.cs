using FluentAssertions;
using KF.Kafka.AdminClient.Internal;
using KF.Kafka.AdminClient.Models;
using KF.Kafka.Configuration.Admin;
using Xunit;

namespace KF.Kafka.AdminClient.Tests;

public sealed class LagSummaryFactoryTests
{
    [Fact]
    public void Healthy_when_totals_under_threshold()
    {
        var lags = new List<ConsumerGroupLag>
        {
            new() { ConsumerGroupId = "g", TopicName = "topic", Partition = 0, EndOffset = 100, CommittedOffset = 95 },
            new() { ConsumerGroupId = "g", TopicName = "topic", Partition = 1, EndOffset = 200, CommittedOffset = 190 }
        };

        var health = new KafkaAdminLagHealthOptions
        {
            HealthyTotalLagThreshold = 20,
            WarningPartitionLagThreshold = 20
        };

        var summary = LagSummaryFactory.Create("group", lags, health);

        summary.IsHealthy.Should().BeTrue();
        summary.TotalLag.Should().Be(15);
        summary.MaxLagPerPartition.Should().Be(10);
        summary.PartitionsWithLag.Should().Be(2);
    }

    [Fact]
    public void Unhealthy_when_partition_exceeds_threshold()
    {
        var lags = new List<ConsumerGroupLag>
        {
            new() { ConsumerGroupId = "g", TopicName = "topic", Partition = 0, EndOffset = 1000, CommittedOffset = 900 }
        };

        var health = new KafkaAdminLagHealthOptions
        {
            HealthyTotalLagThreshold = 5,
            WarningPartitionLagThreshold = 50
        };

        var summary = LagSummaryFactory.Create("group", lags, health);

        summary.IsHealthy.Should().BeFalse();
        summary.TotalLag.Should().Be(100);
        summary.MaxLagPerPartition.Should().Be(100);
    }

    [Fact]
    public void Empty_lag_collection_is_healthy_and_zeroed()
    {
        var summary = LagSummaryFactory.Create("group", Array.Empty<ConsumerGroupLag>(), new KafkaAdminLagHealthOptions());

        summary.TotalLag.Should().Be(0);
        summary.MaxLagPerPartition.Should().Be(0);
        summary.PartitionsWithLag.Should().Be(0);
        summary.TotalPartitions.Should().Be(0);
        summary.IsHealthy.Should().BeTrue();
    }

    [Fact]
    public void Zero_lag_partitions_are_counted_but_not_flagged()
    {
        var lags = new List<ConsumerGroupLag>
        {
            new() { ConsumerGroupId = "g", TopicName = "topic", Partition = 0, EndOffset = 100, CommittedOffset = 100 },
            new() { ConsumerGroupId = "g", TopicName = "topic", Partition = 1, EndOffset = 150, CommittedOffset = 120 }
        };

        var health = new KafkaAdminLagHealthOptions
        {
            HealthyTotalLagThreshold = 40,
            WarningPartitionLagThreshold = 40
        };

        var summary = LagSummaryFactory.Create("group", lags, health);

        summary.TotalLag.Should().Be(30);
        summary.PartitionsWithLag.Should().Be(1);
        summary.TotalPartitions.Should().Be(2);
        summary.IsHealthy.Should().BeTrue();
    }
}
