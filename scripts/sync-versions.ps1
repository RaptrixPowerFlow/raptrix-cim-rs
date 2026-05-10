param(
    [string]$Version,
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RootPackageVersion {
    $cargo = Get-Content -Raw "Cargo.toml"
    $m = [regex]::Match($cargo, '(?ms)^\[package\].*?^version\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"')
    if (-not $m.Success) {
        throw "Could not locate [package] version in Cargo.toml"
    }
    return $m.Groups[1].Value
}

function Convert-ContentForVersion {
    param(
        [string]$Path,
        [string]$Content,
        [string]$TargetVersion
    )

    $updated = $Content

    switch ($Path) {
        "Cargo.toml" {
            $updated = [regex]::Replace($updated, '(?ms)(^\[package\].*?^version\s*=\s*")[^"]+(")', "`${1}$TargetVersion`${2}", 1)
            $updated = [regex]::Replace($updated, '(?m)(raptrix-cim-arrow\s*=\s*\{[^\n]*version\s*=\s*")[^"]+(")', "`${1}$TargetVersion`${2}", 1)
        }
        "raptrix-cim-arrow/Cargo.toml" {
            $updated = [regex]::Replace($updated, '(?ms)(^\[package\].*?^version\s*=\s*")[^"]+(")', "`${1}$TargetVersion`${2}", 1)
        }
        "README.md" {
            $updated = [regex]::Replace(
                $updated,
                '(converter crate release tracks implementation maturity and is currently `)[^`]+(`\.)',
                "`${1}$TargetVersion`${2}",
                1
            )
        }
        "CHANGELOG.md" {
            # Update only the first converter-release line to keep historical sections intact.
            # Use -split '\r?\n', 0 (max 0 = all segments). A second argument of -1 is wrong here in PowerShell.
            $lineEnding = if ($updated.Contains("`r`n")) { "`r`n" } else { "`n" }
            $lines = $updated -split '\r?\n', 0
            for ($i = 0; $i -lt $lines.Length; $i++) {
                if ($lines[$i] -match '^### Converter release: Crate version [0-9]+\.[0-9]+\.[0-9]+ \(raptrix-cim-arrow\) / [0-9]+\.[0-9]+\.[0-9]+ \(raptrix-cim-rs\) \| Arrow schema (v[0-9]+\.[0-9]+\.[0-9]+)$') {
                    $schemaTag = $Matches[1]
                    $lines[$i] = "### Converter release: Crate version $TargetVersion (raptrix-cim-arrow) / $TargetVersion (raptrix-cim-rs) | Arrow schema $schemaTag"
                    break
                }
            }
            $updated = [string]::Join($lineEnding, $lines)
        }
    }

    return $updated
}

if (-not $Version) {
    $Version = Get-RootPackageVersion
}

$files = @(
    "Cargo.toml",
    "raptrix-cim-arrow/Cargo.toml",
    "README.md",
    "CHANGELOG.md"
)

$drift = @()

foreach ($file in $files) {
    $orig = Get-Content -Raw $file
    $new = Convert-ContentForVersion -Path $file -Content $orig -TargetVersion $Version

    if ($orig -ne $new) {
        if ($Check) {
            $drift += $file
        }
        else {
            Set-Content -Path $file -Encoding utf8NoBOM -Value $new
            Write-Host "[sync-versions] updated $file -> $Version"
        }
    }
}

if ($Check -and $drift.Count -gt 0) {
    Write-Error ("Version drift detected in: " + ($drift -join ", ") + ". Run: ./scripts/sync-versions.ps1 -Version $Version")
    exit 1
}

if ($Check) {
    Write-Host "Version consistency checks passed for $Version"
}
