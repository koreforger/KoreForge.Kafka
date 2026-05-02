[CmdletBinding()]
param(
    [string]$TopicName = 'sample-messages',
    [string]$SaPassword = 'YourStrong!Passw0rd'
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$redpanda = 'koreforge-kafka-redpanda'
$sqledge = 'koreforge-kafka-sqledge'

function Wait-ContainerReady {
    param(
        [string]$Name,
        [ScriptBlock]$Probe,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (& $Probe) {
            Write-Host "Container '$Name' is ready."
            return
        }

        Start-Sleep -Seconds 3
    }

    throw "Timed out waiting for container '$Name' to become ready."
}

function Test-Redpanda {
    & docker exec $redpanda rpk cluster info *> $null
    return $LASTEXITCODE -eq 0
}

function Test-SqlEdge {
    & docker exec $sqledge /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SaPassword -Q "SELECT 1" *> $null
    return $LASTEXITCODE -eq 0
}

Wait-ContainerReady -Name $redpanda -Probe ${function:Test-Redpanda}
Wait-ContainerReady -Name $sqledge -Probe ${function:Test-SqlEdge}

Write-Host 'Creating/merging SQL schema + seed data...'
& docker exec $sqledge /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SaPassword -i /init/init.sql
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to execute SQL initialization script.'
}

Write-Host "Ensuring topic '$TopicName' exists..."
$listOutput = & docker exec $redpanda rpk topic list
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to list Kafka topics.'
}

$topicExists = $false
foreach ($line in $listOutput) {
    if ($line -match "^$TopicName\b") {
        $topicExists = $true
        break
    }
}

if (-not $topicExists) {
    & docker exec $redpanda rpk topic create $TopicName --partitions 1 --replicas 1
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to ensure Kafka topic '$TopicName'."
    }
    Write-Host "Topic '$TopicName' created."
}
else {
    Write-Host "Topic '$TopicName' already exists."
}

Write-Host 'Kafka + SQL configuration complete.'
