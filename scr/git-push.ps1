[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$Message
)

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    git add -A
    git commit -m $Message
    git push
} finally {
    Pop-Location
}