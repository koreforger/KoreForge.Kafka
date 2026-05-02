[CmdletBinding()]
param(
    [string]$TopicName = 'sample-messages',
    [string]$SaPassword = 'YourStrong!Passw0rd'
)

$ErrorActionPreference = 'Stop'
$containers = @('koreforge-kafka-redpanda', 'koreforge-kafka-sqledge')

foreach ($name in $containers) {
    $status = (& docker inspect -f '{{.State.Status}}' $name 2>$null).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Container '$name' not found."
    }

    if ($status -ne 'running') {
        throw "Container '$name' is not running (status=$status)."
    }

    Write-Host "Container '$name' is running."
}

Write-Host 'Verifying SQL schema...'
$sqlResult = & docker exec koreforge-kafka-sqledge /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SaPassword -d MultiAppSettings -h -1 -W -Q "SELECT COUNT(*) FROM sys.tables WHERE name = 'Settings' AND schema_id = SCHEMA_ID('dbo');"
if ($LASTEXITCODE -ne 0) {
    throw 'Failed to query SQL schema.'
}

$tableValue = $sqlResult | Where-Object { $_ -match '^[0-9]+$' } | Select-Object -First 1
if (-not $tableValue) {
    throw 'Unexpected SQL output while checking schema.'
}

[int]$tableCount = $tableValue.Trim()
if ($tableCount -lt 1) {
    throw 'Settings table was not found in MultiAppSettings database.'
}

Write-Host 'Settings table exists.'

Write-Host "Verifying Kafka topic '$TopicName'..."
$topics = & docker exec koreforge-kafka-redpanda rpk topic list
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to list Kafka topics.'
}

$topicFound = $false
foreach ($line in $topics) {
    if ($line -match "^$TopicName\b") {
        $topicFound = $true
        break
    }
}

if (-not $topicFound) {
    throw "Kafka topic '$TopicName' does not exist."
}

Write-Host "Kafka topic '$TopicName' exists."
Write-Host 'Docker infrastructure validation succeeded.'
