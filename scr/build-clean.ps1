[CmdletBinding()]
param()

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    dotnet clean KoreForge.Kafka.slnx --verbosity minimal
    Remove-Item 'out' -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host 'Clean complete.' -ForegroundColor Green
} finally {
    Pop-Location
}