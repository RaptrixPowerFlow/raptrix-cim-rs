# Point Cargo / rust-analyzer at C:\build-cache on Windows OneDrive checkouts.
# No-op on macOS / Linux. Does not put a junction in the repo (OneDrive follows
# those). CMake in raptrix-core resolves the real target dir via cargo metadata.

param(
    [string]$RepoRoot = '',
    [switch]$PruneDev = $false,
    [switch]$Help = $false
)

if ($Help) {
    Write-Host @"
Usage: .\scripts\ensure-cargo-target-dir.ps1 [-PruneDev]

On Windows, create C:\build-cache\cargo\raptrix-cim-rs so Cargo/rust-analyzer
do not dump tens of GB into OneDrive. Does not create a repo/target junction
(OneDrive would try to sync it).

-PruneDev runs ``cargo clean --profile dev`` on that cache (keeps release).
No-op on non-Windows.
"@
    exit 0
}

$ErrorActionPreference = 'Stop'
if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

if ($PSVersionTable.PSVersion.Major -ge 6 -and -not $IsWindows) {
    return
}

$TargetDirWin = 'C:\build-cache\cargo\raptrix-cim-rs'
New-Item -ItemType Directory -Force -Path $TargetDirWin | Out-Null

$configPath = Join-Path $RepoRoot '.cargo\config.toml'
if (Test-Path -LiteralPath $configPath) {
    $raw = Get-Content -Raw -LiteralPath $configPath
    if ($raw -notmatch '(?m)^\s*target-dir\s*=') {
        Add-Content -LiteralPath $configPath -Value "`n[build]`ntarget-dir = `"C:/build-cache/cargo/raptrix-cim-rs`""
        Write-Host "Added target-dir to $configPath" -ForegroundColor Green
    }
}

$repoTarget = Join-Path $RepoRoot 'target'
if (Test-Path -LiteralPath $repoTarget) {
    $item = Get-Item -LiteralPath $repoTarget -Force
    $isReparse = [bool]($item.Attributes -band [IO.FileAttributes]::ReparsePoint)
    if ($isReparse) {
        Write-Host "Removing OneDrive-unfriendly target junction (cache stays at $TargetDirWin)"
        cmd /c "rmdir `"$repoTarget`"" | Out-Null
    } else {
        Write-Warning "repo/target exists as a real directory on OneDrive. Delete it; Cargo now writes to $TargetDirWin."
    }
}

Write-Host "Cargo target-dir: $TargetDirWin"

if ($PruneDev) {
    Push-Location $RepoRoot
    try {
        cargo clean --profile dev
        Write-Host "Pruned debug artifacts under $TargetDirWin" -ForegroundColor Green
    } finally {
        Pop-Location
    }
}
