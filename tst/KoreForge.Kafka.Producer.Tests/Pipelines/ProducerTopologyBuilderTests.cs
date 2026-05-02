using System;
using System.Collections.Generic;
using Confluent.Kafka;
using FluentAssertions;
using KoreForge.Kafka.Configuration.Options;
using KoreForge.Kafka.Configuration.Producer;
using KoreForge.Kafka.Configuration.Runtime;
using KoreForge.Kafka.Producer.Pipelines;
using Xunit;

namespace KoreForge.Kafka.Producer.Tests.Pipelines;

public sealed class ProducerTopologyBuilderTests
{
    [Fact]
    public void BuildTopics_Throws_WhenNoTopics()
    {
        var config = new KafkaProducerRuntimeConfig(new ProducerConfig(), new ExtendedProducerSettings(), Array.Empty<string>());

        Action action = () => ProducerTopologyBuilder.BuildTopics(config);

        action.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void BuildTopics_UsesDefaults_WhenOverridesMissing()
    {
        var defaults = new ProducerTopicDefaults
        {
            WorkerCount = 3,
            ChannelCapacity = 250,
            BackpressureMode = ProducerBackpressureMode.Fail
        };
        var extended = new ExtendedProducerSettings
        {
            TopicDefaults = defaults,
            Backlog = new ProducerBacklogSettings { PersistenceEnabled = true }
        };
        var config = new KafkaProducerRuntimeConfig(new ProducerConfig(), extended, new List<string> { "topic-a" });

        var topics = ProducerTopologyBuilder.BuildTopics(config);

        topics.Should().ContainSingle();
        topics[0].TopicName.Should().Be("topic-a");
        topics[0].WorkerCount.Should().Be(3);
        topics[0].ChannelCapacity.Should().Be(250);
        topics[0].BackpressureMode.Should().Be(ProducerBackpressureMode.Fail);
        topics[0].BacklogSettings.PersistenceEnabled.Should().BeTrue();
    }

    [Fact]
    public void BuildTopics_AppliesPerTopicOverrides()
    {
        var extended = new ExtendedProducerSettings
        {
            TopicDefaults = new ProducerTopicDefaults
            {
                WorkerCount = 1,
                ChannelCapacity = 10,
                BackpressureMode = ProducerBackpressureMode.Block
            },
            Backlog = new ProducerBacklogSettings { Directory = "default" },
            Topics = new Dictionary<string, ProducerTopicSettings>(StringComparer.OrdinalIgnoreCase)
            {
                ["topic-b"] = new ProducerTopicSettings
                {
                    WorkerCount = 5,
                    ChannelCapacity = 500,
                    BackpressureMode = ProducerBackpressureMode.Drop,
                    Backlog = new ProducerBacklogSettings { Directory = "custom" }
                }
            }
        };
        var config = new KafkaProducerRuntimeConfig(new ProducerConfig(), extended, new List<string> { "topic-b" });

        var topics = ProducerTopologyBuilder.BuildTopics(config);

        topics.Should().ContainSingle();
        var topic = topics[0];
        topic.WorkerCount.Should().Be(5);
        topic.ChannelCapacity.Should().Be(500);
        topic.BackpressureMode.Should().Be(ProducerBackpressureMode.Drop);
        topic.BacklogSettings.Directory.Should().Be("custom");
    }
}
