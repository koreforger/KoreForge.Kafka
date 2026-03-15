[CmdletBinding()]
param(
    [switch]$RemoveVolumes
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$tasks = @(
    @{ Title = 'Stop Docker Compose'; Command = $(if ($RemoveVolumes) { 'docker compose down -v' } else { 'docker compose down' }) },
    @{ Title = 'Show Container Status'; Command = 'docker ps -a --filter "name=kf-kafka-"' }
)

foreach ($task in $tasks)
{
    $command = "& { $Host.UI.RawUI.WindowTitle = 'KF.Kafka Docker - $($task.Title)'; Set-Location `"$scriptDir`"; Write-Host `"*** $($task.Title) ***`"; $($task.Command); Write-Host `"--- Completed $($task.Title). Press Enter to close this window. ---`"; Read-Host }"
    Start-Process powershell -ArgumentList '-NoExit', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', $command -WindowStyle Normal
    Start-Sleep -Seconds 1
}
