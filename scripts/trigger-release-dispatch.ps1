#Requires -Version 7.2
<#
.SYNOPSIS
  Triggers the GitHub Actions "Release" workflow (release.yml) via API.

.DESCRIPTION
  Use when you need a full binary release without pushing a tag from git.
  Requires a PAT with access to this repository:
  - Fine-grained: Contents (read/write), Actions (read/write), Metadata (read)
  - Classic: repo, workflow

  Set GH_TOKEN or GITHUB_TOKEN in the environment to a PAT that can access
  RaptrixPowerFlow/raptrix-cim-rs (org-owned repos need the token granted to the org).

.EXAMPLE
  $env:GH_TOKEN = (Get-Content ~\secrets\raptrix_gh_pat.txt -Raw).Trim()
  ./scripts/trigger-release-dispatch.ps1 -Version 0.3.4
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$Version,

    [string]$Owner = "RaptrixPowerFlow",
    [string]$Repo = "raptrix-cim-rs",

    [switch]$Draft,

    [switch]$SkipPublish
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$token = $env:GH_TOKEN ?? $env:GITHUB_TOKEN
if (-not $token) {
    throw "Set GH_TOKEN or GITHUB_TOKEN to a PAT with repo + Actions access for $Owner/$Repo"
}

$headers = @{
    Authorization          = "Bearer $token"
    Accept                   = "application/vnd.github+json"
    "X-GitHub-Api-Version"   = "2022-11-28"
}

$uri = "https://api.github.com/repos/$Owner/$Repo/actions/workflows/release.yml/dispatches"
$publishStr = if ($SkipPublish) { "false" } else { "true" }
$draftStr = if ($Draft) { "true" } else { "false" }
$bodyObj = @{
    ref    = "main"
    inputs = @{
        version         = $Version
        publish_release = $publishStr
        draft             = $draftStr
    }
}
$body = $bodyObj | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -Headers $headers -Uri $uri -Body $body -ContentType "application/json"
Write-Host "Dispatched Release workflow on main for version $Version (draft=$($Draft.IsPresent), publish_release=$(-not $SkipPublish))."
