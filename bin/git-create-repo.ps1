[CmdletBinding()]
param()

$RepoName = 'KoreForge.Kafka'
$Owner    = 'koreforger'

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    if (-not (Test-Path '.git')) {
        git init
        git add -A
        git commit -m 'chore: initial commit'
    }
    gh repo create "$Owner/$RepoName" --public --source . --remote origin --push
} finally {
    Pop-Location
}