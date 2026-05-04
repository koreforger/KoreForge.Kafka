# KoreForge.Kafka – Usage Guide

## 1. Configuration

Define a Kafka profile in `appsettings.json`:

```json
{
  "Kafka": {
    "Profiles": {
      "Default": {
        "BootstrapServers": "localhost:29092",
        "SecurityProtocol": "Plaintext"
      }
    }
  }
}
```

Register the configuration in your DI container:

```csharp
services.AddKafkaConfiguration(configuration.GetSection("Kafka"));
```

## 2. Producer

```csharp
services.AddKafkaProducer<string, MyEvent>(options =>
{
    options.ProfileName = "Default";
    options.Topic = "my-events";
});
```

Inject `IKafkaProducer<string, MyEvent>` and call `EnqueueAsync(key, value)`.

## 3. Consumer

```csharp
services.AddKafkaConsumer<string, MyEvent>(options =>
{
    options.ProfileName = "Default";
    options.Topics = new[] { "my-events" };
    options.GroupId = "my-service";
});
```

Implement `IKafkaMessageHandler<string, MyEvent>` to process messages.

## 4. Admin Client

```csharp
services.AddKafkaAdminClient(configuration.GetSection("Kafka:Admin"));
```

Inject `IKafkaAdminClient` for topic metadata and consumer-lag queries:

```csharp
var lag = await adminClient.GetConsumerGroupLagSummaryAsync(
    "my-service",
    new ConsumerGroupLagQuery { TopicFilter = new[] { "my-events" } });
```

## 5. Docker dev environment

Start a local Kafka + SQL instance:

```powershell
.\samples\docker\up.ps1
```

This starts Redpanda (Kafka-compatible) on `localhost:29092` and Azure SQL Edge on `localhost:14333`.

Stop:

```powershell
.\samples\docker\down.ps1
```
