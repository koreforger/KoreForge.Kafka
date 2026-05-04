# KoreForge.Kafka.AdminClient

Read-only Kafka administration surface used by the consumer / producer hosts. The library intentionally mirrors the conventions described in `docs/Kafka Admin Client.md`.

## Features

- Async API via `IKafkaAdminClient` for topic metadata, consumer-lag snapshots, and offsets-for-timestamp lookups.
- Config-only behaviour through `KafkaAdminOptions` (bootstrap servers, retries, caching, health thresholds, security, etc.).
- Fluent builder (`KafkaAdminClientHost.Create()...`) and DI extension (`services.AddKafkaAdminClient(configuration)`).
- Optional metrics bridge via `IKafkaAdminMetrics`; when `IOperationMonitor` is registered (via `AddKFMetrics`), an adapter is wired automatically.
- Bounded TTL caches for metadata and lag summaries.
- Retry/backoff policy driven entirely by configuration.

## Quick start

```csharp
var services = new ServiceCollection();
services.AddKafkaAdminClient(configuration.GetSection("Kafka:Admin"));

var provider = services.BuildServiceProvider();
var admin = provider.GetRequiredService<IKafkaAdminClient>();

var metadata = await admin.GetTopicMetadataAsync("payments");
var lag = await admin.GetConsumerGroupLagSummaryAsync(
    "payments-consumer",
    new ConsumerGroupLagQuery { TopicFilter = new[] { "payments" } });
```

### Using Kafka configuration profiles

If your application already binds the shared `KoreForge.Kafka.Configuration` package, feed the validated profile straight into the builder:

```csharp
var configFactory = services.BuildServiceProvider().GetRequiredService<IKafkaClientConfigFactory>();

var adminClient = KafkaAdminClientHost
    .Create()
    .UseKafkaConfigurationProfile("AdminPrimary", configFactory)
    .UseLoggerFactory(loggerFactory)
    .UseMetrics(metrics)
    .Build();
```

`UseKafkaConfigurationProfile` copies the Confluent `AdminClientConfig` into the admin builder so bootstrap servers, client id, SSL/SASL settings, and request timeouts stay consistent with the rest of the Kafka stack.

### Metrics

`AddKafkaAdminClient` will reuse any registered `IOperationMonitor` (from `KoreForge.Metrics`) and emit the following operations out-of-the-box:

- `kafka.admin.metadata`, `kafka.admin.lag`, `kafka.admin.offsets-for-timestamp` – duration + failure tracking for each broker call.
- `kafka.admin.errors` – per-operation error counters tagged with the exception type.
- `kafka.admin.cache.hit` / `kafka.admin.cache.miss` – hit/miss counters tagged with the cache name.

To opt out or plug in a different sink, register your own `IKafkaAdminMetrics` before calling `AddKafkaAdminClient`.
