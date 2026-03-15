# KoreForge.Kafka – Samples: Docker Infrastructure

Local development environment for running integration tests against a real Kafka broker and SQL database.

## What's included

| Container | Image | Port |
|---|---|---|
| `kf-kafka-redpanda` | Redpanda v24.1 (Kafka-compatible) | `29092` |
| `kf-kafka-sqledge` | Azure SQL Edge (SQL Server compatible) | `14333` |

## Usage

### Start

```powershell
# Starts containers, creates the Kafka topic, and seeds the SQL schema
.\up.ps1
```

### Stop

```powershell
.\down.ps1

# Remove volumes (full reset)
.\down.ps1 -RemoveVolumes
```

### Validate

```powershell
.\test-docker.ps1
```

### Manual container management

```powershell
# Start containers only (no configuration)
.\start-docker.ps1

# Configure topic + SQL schema after containers are running
.\configure-infra.ps1
```

## Configuration details

- Kafka topic: `sample-messages` (1 partition, 1 replica)
- SQL database: `MultiAppSettings` with a `Settings` table seeded with publisher/consumer config

## Integration test setup

Integration test projects (`*.IntegrationTests`) reference `Testcontainers.Kafka` for isolated
per-test Kafka instances and do not require this Docker environment. The Docker setup is
for exploratory testing or multi-component integration scenarios.
