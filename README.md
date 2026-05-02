# KoreForge.Kafka

A production-grade Kafka client stack for .NET 10, built on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet).

## Packages

| Package | Description |
|---|---|
| `KoreForge.Kafka` | Meta-package — installs all components below |
| `KoreForge.Kafka.AdminClient` | Read-only admin surface: topic metadata, consumer-lag, offsets-for-timestamp |
| `KoreForge.Kafka.Configuration` | Configuration model, profiles, validation, generated factories |
| `KoreForge.Kafka.Configuration.AspNetCore` | ASP.NET Core integration for configuration |
| `KoreForge.Kafka.Consumer` | Resilient consumer host with backpressure, routing, pipeline, and restart |
| `KoreForge.Kafka.Core` | Shared runtime records, alert engine, and diagnostics |
| `KoreForge.Kafka.Producer` | Resilient producer host with buffering, backpressure, backlog, and metrics |

## Quick start

Install the meta-package to get everything:
```
dotnet add package KoreForge.Kafka
```

Or install individual packages for leaner dependencies:
```
dotnet add package KoreForge.Kafka.Consumer
dotnet add package KoreForge.Kafka.Producer
dotnet add package KoreForge.Kafka.AdminClient
```

See [doc/UsageGuide.md](doc/UsageGuide.md) for detailed usage examples.

## Prerequisites

- .NET 10 SDK
- A running Kafka broker (see [samples/docker](samples/docker) for a local dev environment)

## Development

```powershell
# Clean
.\scr\build-clean.ps1

# Build and test
.\scr\build-test.ps1

# Build, test, and generate coverage report
.\scr\build-test-codecoverage.ps1

# Pack NuGet
.\scr\build-pack.ps1
```

## Docker dev infrastructure

The `samples/docker` directory contains a Docker Compose setup with:
- **Redpanda** (Kafka-compatible broker) on port `29092`
- **Azure SQL Edge** on port `14333`

```powershell
# Start and configure infrastructure
.\samples\docker\up.ps1

# Stop
.\samples\docker\down.ps1
```

## Solution layout

```
src/
  KoreForge.Kafka/                           Meta-package (bundles all components)
  KoreForge.Kafka.AdminClient/               Admin client library
  KoreForge.Kafka.Configuration/             Configuration library
  KoreForge.Kafka.Configuration.AspNetCore/  ASP.NET Core configuration extension
  KoreForge.Kafka.Consumer/                  Consumer library
  KoreForge.Kafka.Core/                      Core shared types
  KoreForge.Kafka.Producer/                  Producer library
tst/
  KoreForge.Kafka.AdminClient.Tests/
  KoreForge.Kafka.AdminClient.IntegrationTests/
  KoreForge.Kafka.Configuration.Tests/
  KoreForge.Kafka.Consumer.Tests/
  KoreForge.Kafka.Consumer.IntegrationTests/
  KoreForge.Kafka.Core.Tests/
  KoreForge.Kafka.Producer.Tests/
  KoreForge.Kafka.Producer.IntegrationTests/
samples/
  docker/                             Local dev infrastructure (Redpanda + SQL)
scr/
  build-*.ps1                         Automation scripts
```

## License

MIT — see [LICENSE.md](LICENSE.md).
