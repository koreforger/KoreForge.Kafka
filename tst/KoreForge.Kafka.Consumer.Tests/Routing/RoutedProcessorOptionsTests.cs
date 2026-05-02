using System;
using FluentAssertions;
using KoreForge.Kafka.Consumer.Routing;
using Xunit;

namespace KoreForge.Kafka.Consumer.Tests.Routing;

public sealed class RoutedProcessorOptionsTests
{
    [Fact]
    public void Validate_passes_with_default_options()
    {
        var options = new RoutedProcessorOptions();

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_throws_for_invalid_ConsumerThreadCount(int count)
    {
        var options = new RoutedProcessorOptions { ConsumerThreadCount = count };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.ConsumerThreadCount));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_throws_for_invalid_BucketCount(int count)
    {
        var options = new RoutedProcessorOptions { BucketCount = count };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.BucketCount));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(7)]
    [InlineData(12)]
    [InlineData(100)]
    public void Validate_throws_for_non_power_of_two_BucketCount(int count)
    {
        var options = new RoutedProcessorOptions { BucketCount = count };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>()
            .And.ParamName.Should().Be(nameof(options.BucketCount));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(64)]
    [InlineData(128)]
    public void Validate_accepts_power_of_two_BucketCount(int count)
    {
        var options = new RoutedProcessorOptions { BucketCount = count };

        var act = () => options.Validate();

        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_throws_for_invalid_BucketQueueCapacity(int capacity)
    {
        var options = new RoutedProcessorOptions { BucketQueueCapacity = capacity };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.BucketQueueCapacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_throws_for_invalid_MaxBatchSize(int size)
    {
        var options = new RoutedProcessorOptions { MaxBatchSize = size };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.MaxBatchSize));
    }

    [Fact]
    public void Validate_throws_for_zero_BatchTimeout()
    {
        var options = new RoutedProcessorOptions { BatchTimeout = TimeSpan.Zero };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.BatchTimeout));
    }

    [Fact]
    public void Validate_throws_for_negative_BatchTimeout()
    {
        var options = new RoutedProcessorOptions { BatchTimeout = TimeSpan.FromSeconds(-1) };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.BatchTimeout));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(101)]
    public void Validate_throws_for_invalid_BackpressureThresholdPercent(int percent)
    {
        var options = new RoutedProcessorOptions { BackpressureThresholdPercent = percent };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.BackpressureThresholdPercent));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(100)]
    public void Validate_throws_for_invalid_BackpressureResumePercent(int percent)
    {
        var options = new RoutedProcessorOptions { BackpressureResumePercent = percent };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .And.ParamName.Should().Be(nameof(options.BackpressureResumePercent));
    }

    [Fact]
    public void Validate_throws_when_ResumePercent_not_less_than_ThresholdPercent()
    {
        var options = new RoutedProcessorOptions
        {
            BackpressureThresholdPercent = 80,
            BackpressureResumePercent = 80
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Validate_throws_when_ResumePercent_greater_than_ThresholdPercent()
    {
        var options = new RoutedProcessorOptions
        {
            BackpressureThresholdPercent = 70,
            BackpressureResumePercent = 80
        };

        var act = () => options.Validate();

        act.Should().Throw<ArgumentException>();
    }
}
