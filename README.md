# KoreForge.Kafka

A production-grade Kafka client stack for .NET 10, built on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet).

## Packages

| Package | Description |
|---|---|
| `KF.Kafka` | Meta-package — installs all components below |
| `KF.Kafka.AdminClient` | Read-only admin surface: topic metadata, consumer-lag, offsets-for-timestamp |
| `KF.Kafka.Configuration` | Configuration model, profiles, validation, generated factories |
| `KF.Kafka.Configuration.AspNetCore` | ASP.NET Core integration for configuration |
| `KF.Kafka.Consumer` | Resilient consumer host with backpressure, routing, pipeline, and restart |
| `KF.Kafka.Core` | Shared runtime records, alert engine, and diagnostics |
| `KF.Kafka.Producer` | Resilient producer host with buffering, backpressure, backlog, and metrics |

## Quick start

Install the meta-package to get everything:
```
dotnet add package KF.Kafka
```

Or install individual packages for leaner dependencies:
```
dotnet add package KF.Kafka.Consumer
dotnet add package KF.Kafka.Producer
dotnet add package KF.Kafka.AdminClient
```

See [doc/UsageGuide.md](doc/UsageGuide.md) for detailed usage examples.

## Prerequisites

- .NET 10 SDK
- A running Kafka broker (see [samples/docker](samples/docker) for a local dev environment)

## Development

```powershell
# Clean
.\bin\build-clean.ps1

# Build and test
.\bin\build-test.ps1

# Build, test, and generate coverage report
.\bin\build-test-codecoverage.ps1

# Pack NuGet
.\bin\build-pack.ps1
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
  KF.Kafka/                           Meta-package (bundles all components)
  KF.Kafka.AdminClient/               Admin client library
  KF.Kafka.Configuration/             Configuration library
  KF.Kafka.Configuration.AspNetCore/  ASP.NET Core configuration extension
  KF.Kafka.Consumer/                  Consumer library
  KF.Kafka.Core/                      Core shared types
  KF.Kafka.Producer/                  Producer library
tst/
  KF.Kafka.AdminClient.Tests/
  KF.Kafka.AdminClient.IntegrationTests/
  KF.Kafka.Configuration.Tests/
  KF.Kafka.Consumer.Tests/
  KF.Kafka.Consumer.IntegrationTests/
  KF.Kafka.Core.Tests/
  KF.Kafka.Producer.Tests/
  KF.Kafka.Producer.IntegrationTests/
samples/
  docker/                             Local dev infrastructure (Redpanda + SQL)
bin/
  build-*.ps1                         Automation scripts
```

## License

MIT — see [LICENSE.md](LICENSE.md).
