param(
    [switch]$SkipStart,
    [switch]$KeepRunning,
    [switch]$SkipBuild,
    [string]$Python = "python",
    [string]$WheelDir,
    [string]$NpmPackage
)

$ErrorActionPreference = "Stop"
$arguments = @((Join-Path $PSScriptRoot "integration-smoke.py"))
if ($SkipStart) { $arguments += "--skip-start" }
if ($KeepRunning) { $arguments += "--keep" }
if ($SkipBuild) { $arguments += "--skip-build" }
if ($WheelDir) { $arguments += @("--wheel-dir", $WheelDir) }
if ($NpmPackage) { $arguments += @("--npm-package", $NpmPackage) }
& $Python @arguments
if ($LASTEXITCODE -ne 0) { throw "Production integration failed with exit code $LASTEXITCODE" }
