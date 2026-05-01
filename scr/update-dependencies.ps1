#Requires -Version 7.2
<#
.SYNOPSIS
    Updates cross-repo KoreForge dependency version pins in Directory.Packages.props.

.DESCRIPTION
    KoreForge.Kafka depends on several KoreForge sibling packages (Time, Logging,
    Metrics, Processing). The default version pin is stored as a fallback in
    Directory.Packages.props. This script updates that pin to match the version
    being released so that dotnet pack produces stable (non-prerelease) references.

.PARAMETER Version
    The KoreForge sibling version to pin (e.g. 1.2.0).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $Version
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$propsFile = Join-Path $PSScriptRoot '..\Directory.Packages.props'
$propsFile = (Resolve-Path $propsFile).Path

Write-Host "Updating KoreForgeVersion pin → $Version" -ForegroundColor Cyan
Write-Host "  File: $propsFile"

$content = Get-Content $propsFile -Raw
$updated = $content -replace `
    '(<KoreForgeVersion Condition="''\$\(KoreForgeVersion\)'' == ''''">)[^<]*(</KoreForgeVersion>)', `
    "`${1}$Version`${2}"

if ($content -eq $updated) {
    Write-Warning "No KoreForgeVersion fallback line found — nothing updated."
}
else {
    Set-Content $propsFile $updated -NoNewline
    Write-Host "  ✓ KoreForgeVersion → $Version" -ForegroundColor Green
}
