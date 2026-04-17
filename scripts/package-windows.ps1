param(
    [Parameter(Mandatory = $true)]
    [string]$Target,
    [Parameter(Mandatory = $true)]
    [string]$ArchLabel,
    [Parameter(Mandatory = $true)]
    [string]$Version
)

$ErrorActionPreference = "Stop"

$binName = "raptrix-cim-rs.exe"
$distDir = "dist"
$pkgRoot = Join-Path $distDir "raptrix-cim-rs-v$Version-windows-$ArchLabel"

New-Item -ItemType Directory -Force -Path $pkgRoot | Out-Null
Copy-Item "target\$Target\release\$binName" -Destination (Join-Path $pkgRoot $binName) -Force
Copy-Item "README.md" -Destination (Join-Path $pkgRoot "README.md") -Force
Copy-Item "LICENSE" -Destination (Join-Path $pkgRoot "LICENSE") -Force

$zipPath = Join-Path $distDir "raptrix-cim-rs-v$Version-windows-$ArchLabel.zip"
if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}
Compress-Archive -Path (Join-Path $pkgRoot "*") -DestinationPath $zipPath
Write-Host "Created $zipPath"
