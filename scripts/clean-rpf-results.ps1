param(
    [switch]$KeepDebug
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$resultsRoot = Join-Path $repoRoot "tests\data\external\results"
$debugDir = Join-Path $resultsRoot "debug"

if (-not (Test-Path $resultsRoot)) {
    Write-Host "No results directory found at: $resultsRoot"
    exit 0
}

if (-not $KeepDebug -and (Test-Path $debugDir)) {
    Remove-Item -Recurse -Force $debugDir
    Write-Host "Removed non-canonical debug results: $debugDir"
} else {
    Write-Host "Keeping debug results."
}

Write-Host "Canonical golden path retained: $resultsRoot\release"