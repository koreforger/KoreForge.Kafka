[CmdletBinding()]
param(
    [switch]$Recreate
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Push-Location $scriptDir
try {
    if ($Recreate) {
        Write-Host 'Recreating containers (docker compose down -v)...'
        docker compose down -v
    }

    Write-Host 'Starting Kafka + SQL containers...'
    docker compose up -d | Out-Null
    docker compose ps
}
finally {
    Pop-Location
}
