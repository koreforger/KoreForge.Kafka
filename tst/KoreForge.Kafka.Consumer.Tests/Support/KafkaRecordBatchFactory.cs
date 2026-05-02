using System;
using System.Collections.Generic;
using Confluent.Kafka;
using KoreForge.Kafka.Consumer.Batch;

namespace KoreForge.Kafka.Consumer.Tests.Support;

internal static class KafkaRecordBatchFactory
{
    public static KafkaRecordBatch CreateBatch(params int[] payloads)
    {
        var records = new List<ConsumeResult<byte[], byte[]>>(payloads.Length);
        for (var i = 0; i < payloads.Length; i++)
        {
            var topicPartition = new TopicPartition("topic", new Partition(i));
            var offset = new TopicPartitionOffset(topicPartition, new Offset(payloads[i]));
            var record = new ConsumeResult<byte[], byte[]>
            {
                TopicPartitionOffset = offset,
                Message = new Message<byte[], byte[]>
                {
                    Key = BitConverter.GetBytes(i),
                    Value = BitConverter.GetBytes(payloads[i]),
                    Timestamp = new Timestamp(DateTime.UtcNow),
                    Headers = new Headers { new Header("x-test", new byte[] { 1 }) }
                }
            };

            records.Add(record);
        }

        return new KafkaRecordBatch(records, DateTimeOffset.UtcNow);
    }
}
