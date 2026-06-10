param(
    [string]$Version,
    [string]$SchemaVersion,
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

function Get-SchemaVersion {
    $schema = Get-Content -Raw "raptrix-cim-arrow/src/schema.rs"
    $m = [regex]::Match($schema, 'pub const RPF_VERSION: &str = "(v[0-9]+\.[0-9]+\.[0-9]+)";')
    if (-not $m.Success) {
        throw "Could not locate RPF_VERSION in raptrix-cim-arrow/src/schema.rs"
    }
    return $m.Groups[1].Value
}

function Convert-ContentForVersion {
    param(
        [string]$Path,
        [string]$Content,
        [string]$TargetVersion,
        [string]$TargetSchemaVersion
    )

    $updated = $Content
    $schemaPlain = $TargetSchemaVersion.TrimStart('v')

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
            $updated = [regex]::Replace(
                $updated,
                '(schema contract v)[0-9]+\.[0-9]+\.[0-9]+',
                "`${1}$schemaPlain",
                1
            )
            $updated = [regex]::Replace(
                $updated,
                '(file-format contract is at schema \*\*`)[^`]+(`\*\* while the converter crate release is \*\*`)[^`]+(`\*\*)',
                "`${1}$TargetSchemaVersion`${2}$TargetVersion`${3}",
                1
            )
        }
        "CHANGELOG.md" {
            # Update only the first converter-release line to keep historical sections intact.
            $lineEnding = if ($updated.Contains("`r`n")) { "`r`n" } else { "`n" }
            $lines = $updated -split '\r?\n', 0
            for ($i = 0; $i -lt $lines.Length; $i++) {
                if ($lines[$i] -match '^### Converter release: Crate version [0-9]+\.[0-9]+\.[0-9]+ \(raptrix-cim-arrow\) / [0-9]+\.[0-9]+\.[0-9]+ \(raptrix-cim-rs\) \| Arrow schema (v[0-9]+\.[0-9]+\.[0-9]+)$') {
                    $lines[$i] = "### Converter release: Crate version $TargetVersion (raptrix-cim-arrow) / $TargetVersion (raptrix-cim-rs) | Arrow schema $TargetSchemaVersion"
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

if (-not $SchemaVersion) {
    $SchemaVersion = Get-SchemaVersion
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
    $new = Convert-ContentForVersion -Path $file -Content $orig -TargetVersion $Version -TargetSchemaVersion $SchemaVersion

    if ($orig -ne $new) {
        if ($Check) {
            $drift += $file
        }
        else {
            Set-Content -Path $file -Encoding utf8NoBOM -Value $new
            Write-Host "[sync-versions] updated $file -> crate $Version, schema $SchemaVersion"
        }
    }
}

if ($Check -and $drift.Count -gt 0) {
    Write-Error ("Version drift detected in: " + ($drift -join ", ") + ". Run: ./scripts/sync-versions.ps1 -Version $Version -SchemaVersion $SchemaVersion")
    exit 1
}

if ($Check) {
    Write-Host "Version consistency checks passed for crate $Version and schema $SchemaVersion"
}
