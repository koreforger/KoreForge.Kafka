[CmdletBinding()]
param(
    [string]$Configuration = 'Debug',
    [switch]$Open
)

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    dotnet build KoreForge.Kafka.slnx --force -c $Configuration
    dotnet test  KoreForge.Kafka.slnx -c $Configuration --no-build `
        --logger "html;LogFileName=TestResults.html" `
        --results-directory out/TestResults `
        --collect:"XPlat Code Coverage" `
        --settings coverlet.runsettings
    dotnet tool run reportgenerator `
        "-reports:out/TestResults/**/coverage.cobertura.xml" `
        "-targetdir:out/TestResults/coverage" `
        '-reporttypes:Html;Cobertura'
    if ($Open) {
        Invoke-Item (Resolve-Path 'out/TestResults/coverage/index.html')
    }
    Write-Host 'Coverage report: out/TestResults/coverage/index.html' -ForegroundColor Green
} finally {
    Pop-Location
}