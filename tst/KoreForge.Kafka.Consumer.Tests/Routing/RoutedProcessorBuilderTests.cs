using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Routing;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Routing;

public sealed class RoutedProcessorBuilderTests
{
    [Fact]
    public void Build_throws_without_processor()
    {
        var builder = RoutedProcessor.Create<string, string>();

        var act = () => builder.Build();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*processor*");
    }

    [Fact]
    public void Build_succeeds_with_processor()
    {
        var processor = new TestBucketProcessor();

        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(processor)
            .Build();

        host.Should().NotBeNull();
        host.BucketCount.Should().Be(64); // Default
    }

    [Fact]
    public void Build_with_factory_succeeds()
    {
        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(_ => new TestBucketProcessor())
            .Build();

        host.Should().NotBeNull();
    }

    [Fact]
    public void WithConsumerThreads_configures_count()
    {
        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .WithConsumerThreads(16)
            .Build();

        host.ConsumerThreadCount.Should().Be(16);
    }

    [Fact]
    public void WithBucketCount_configures_count()
    {
        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .WithBucketCount(32)
            .Build();

        host.BucketCount.Should().Be(32);
    }

    [Fact]
    public void WithBucketCount_throws_for_non_power_of_two()
    {
        var act = () => RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .WithBucketCount(17)
            .Build();

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Configure_applies_options()
    {
        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .Configure(opts =>
            {
                opts.BucketCount = 16;
                opts.ConsumerThreadCount = 8;
                opts.MaxBatchSize = 500;
            })
            .Build();

        host.BucketCount.Should().Be(16);
        host.ConsumerThreadCount.Should().Be(8);
    }

    [Fact]
    public void WithOptions_applies_options()
    {
        var options = new RoutedProcessorOptions
        {
            BucketCount = 32,
            ConsumerThreadCount = 12,
            BucketQueueCapacity = 25000
        };

        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .WithOptions(options)
            .Build();

        host.BucketCount.Should().Be(32);
        host.ConsumerThreadCount.Should().Be(12);
    }

    [Fact]
    public void WithBackpressure_configures_behavior()
    {
        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .WithBackpressure(BackpressureBehavior.DropMessages, 90, 60)
            .Build();

        host.Should().NotBeNull();
    }

    [Fact]
    public async Task WithHashFunction_is_used()
    {
        var processor = new TestBucketProcessor();

        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(processor)
            .WithBucketCount(4)
            .WithHashFunction(_ => 1) // Always route to bucket 1
            .Build();

        await host.StartAsync();

        for (int i = 0; i < 5; i++)
        {
            await host.RouteAsync(CreateMessage($"key-{i}", $"value-{i}"));
        }

        await Task.Delay(200);
        await host.StopAsync();

        // All should be in bucket 1
        host.GetBucketIndex("any-key").Should().Be(1);
    }

    [Fact]
    public void Fluent_api_chains_correctly()
    {
        var host = RoutedProcessor.Create<string, string>()
            .UseBucketProcessor(new TestBucketProcessor())
            .WithConsumerThreads(4)
            .WithBucketCount(8)
            .WithBucketQueueCapacity(10000)
            .WithMaxBatchSize(250)
            .WithBatchTimeout(TimeSpan.FromMilliseconds(50))
            .WithBackpressure(BackpressureBehavior.PauseConsumption, 85, 45)
            .Build();

        host.BucketCount.Should().Be(8);
        host.ConsumerThreadCount.Should().Be(4);
    }

    private static RoutedMessage<string, string> CreateMessage(string key, string value)
    {
        return new RoutedMessage<string, string>
        {
            RoutingKey = key,
            Message = value,
            Partition = 0,
            Offset = 0,
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    private sealed class TestBucketProcessor : IBucketProcessor<string, string>
    {
        public ValueTask ProcessBatchAsync(
            int bucketIndex,
            IReadOnlyList<RoutedMessage<string, string>> messages,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}
