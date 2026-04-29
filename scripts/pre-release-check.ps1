param(
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"

Write-Host "[pre-release] checking version sync..."
./scripts/sync-versions.ps1 -Check

Write-Host "[pre-release] checking formatting..."
cargo fmt --all -- --check

Write-Host "[pre-release] checking compilation..."
cargo check --workspace --all-targets

if (-not $SkipTests) {
    Write-Host "[pre-release] running tests..."
    cargo test --workspace --all-targets
} else {
    Write-Host "[pre-release] tests skipped by request."
}

Write-Host "[pre-release] all checks passed."
