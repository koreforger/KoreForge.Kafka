using KF.Kafka.AdminClient.Models;
using KF.Kafka.Configuration.Admin;

namespace KF.Kafka.AdminClient.Internal;

internal static class LagSummaryFactory
{
    public static ConsumerGroupLagSummary Create(
        string consumerGroupId,
        IReadOnlyList<ConsumerGroupLag> lags,
        KafkaAdminLagHealthOptions healthOptions)
    {
        var totalLag = lags.Sum(static l => l.Lag);
        var maxLag = lags.Count == 0 ? 0 : lags.Max(static l => l.Lag);
        var partitionsWithLag = lags.Count(static l => l.Lag > 0);

        var isHealthy = totalLag <= healthOptions.HealthyTotalLagThreshold &&
                        maxLag <= healthOptions.WarningPartitionLagThreshold;

        return new ConsumerGroupLagSummary
        {
            ConsumerGroupId = consumerGroupId,
            TotalLag = totalLag,
            MaxLagPerPartition = maxLag,
            PartitionsWithLag = partitionsWithLag,
            TotalPartitions = lags.Count,
            IsHealthy = isHealthy
        };
    }
}
