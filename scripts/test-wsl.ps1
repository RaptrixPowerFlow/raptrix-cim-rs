param(
    [string]$CargoCommand = "test --workspace"
)

$ErrorActionPreference = "Stop"

function Convert-ToWslPath {
    param([Parameter(Mandatory = $true)][string]$WindowsPath)

    $resolved = (Resolve-Path $WindowsPath).Path
    if ($resolved -notmatch '^([A-Za-z]):\\') {
        throw "Unsupported Windows path format: $resolved"
    }

    $drive = $matches[1].ToLowerInvariant()
    $suffix = $resolved.Substring(2).Replace('\', '/')
    return "/mnt/$drive$suffix"
}

$repoRoot = Join-Path $PSScriptRoot ".."
$repoWsl = Convert-ToWslPath -WindowsPath $repoRoot

$command = "cd '$repoWsl' && cargo $CargoCommand"
Write-Host "Running in WSL: $command"

& wsl.exe bash -lc $command
exit $LASTEXITCODE
