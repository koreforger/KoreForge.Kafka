[CmdletBinding()]
param(
    [string]$ApiKey
)

$RepoName = 'KoreForge.Kafka'
$Owner    = 'koreforger'

if (-not $ApiKey) {
    $secure = Read-Host -Prompt 'NUGET_API_KEY' -AsSecureString
    $ApiKey = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
                  [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
}

$ApiKey | gh secret set NUGET_API_KEY --repo "$Owner/$RepoName"
Write-Host "NUGET_API_KEY set on $Owner/$RepoName" -ForegroundColor Green