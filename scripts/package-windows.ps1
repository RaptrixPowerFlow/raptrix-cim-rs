param(
    [Parameter(Mandatory = $true)]
    [string]$Target,
    [Parameter(Mandatory = $true)]
    [string]$ArchLabel,
    [Parameter(Mandatory = $true)]
    [string]$Version
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if ($env:CARGO_TARGET_DIR) {
    $cargoTarget = $env:CARGO_TARGET_DIR
    if (-not [IO.Path]::IsPathRooted($cargoTarget)) {
        $cargoTarget = Join-Path $repoRoot $cargoTarget
    }
} else {
    $cargoTarget = (cargo metadata --format-version 1 --no-deps | ConvertFrom-Json).target_directory
}

$binName = "raptrix-cim-rs.exe"
$distDir = "dist"
$pkgRoot = Join-Path $distDir "raptrix-cim-rs-v$Version-windows-$ArchLabel"
$exePath = Join-Path $cargoTarget "$Target\release\$binName"

New-Item -ItemType Directory -Force -Path $pkgRoot | Out-Null
Copy-Item $exePath -Destination (Join-Path $pkgRoot $binName) -Force
Copy-Item "README.md" -Destination (Join-Path $pkgRoot "README.md") -Force
Copy-Item "LICENSE" -Destination (Join-Path $pkgRoot "LICENSE") -Force

$zipPath = Join-Path $distDir "raptrix-cim-rs-v$Version-windows-$ArchLabel.zip"
if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}
Compress-Archive -Path (Join-Path $pkgRoot "*") -DestinationPath $zipPath
Write-Host "Created $zipPath"
