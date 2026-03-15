[CmdletBinding()]
param(
    [switch]$SkipTest
)

$ErrorActionPreference = 'Stop'
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$tasks = @(
    @{ Title = 'Start Docker Compose'; Script = 'start-docker.ps1'; Arguments = '' },
    @{ Title = 'Configure Kafka / SQL'; Script = 'configure-infra.ps1'; Arguments = '' }
)

if (-not $SkipTest)
{
    $tasks += @{ Title = 'Validate Infrastructure'; Script = 'test-docker.ps1'; Arguments = '' }
}

foreach ($task in $tasks)
{
    $scriptPath = Join-Path $scriptDir $task.Script
    if (-not (Test-Path $scriptPath))
    {
        Write-Warning "Skipping task '$($task.Title)' because script '$($task.Script)' was not found."
        continue
    }

    $invocation = "& `"$scriptPath`" $($task.Arguments)"
    $command = "& { $Host.UI.RawUI.WindowTitle = 'Khaos Docker - $($task.Title)'; Set-Location `"$scriptDir`"; Write-Host `"*** $($task.Title) ***`"; $invocation; Write-Host `"--- Completed $($task.Title). Press Enter to close this window. ---`"; Read-Host }"

    Start-Process powershell -ArgumentList '-NoExit', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', $command -WindowStyle Normal
    Start-Sleep -Seconds 1
}
