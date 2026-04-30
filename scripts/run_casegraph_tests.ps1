# Run the CaseGraph no-live test suite from a stable Python interpreter.
# Bootstraps .venv if missing, installs requirements-dev.txt, then invokes pytest.
#
# Usage:
#   .\scripts\run_casegraph_tests.ps1
#   .\scripts\run_casegraph_tests.ps1 tests/test_casegraph_scoring.py -v
#
# Extra arguments are forwarded to pytest verbatim.

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvDir  = Join-Path $repoRoot '.venv'
$venvPy   = Join-Path $venvDir 'Scripts\python.exe'
$reqFile  = Join-Path $repoRoot 'requirements-dev.txt'

if (-not (Test-Path $venvPy)) {
    Write-Host "Bootstrapping .venv at $venvDir (using py -3.12)" -ForegroundColor Cyan
    & py -3.12 -m venv $venvDir
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create .venv. Install Python 3.12 or run: py -3.12 -m venv $venvDir"
    }
}

Write-Host "Syncing requirements-dev.txt" -ForegroundColor Cyan
& $venvPy -m pip install --quiet --upgrade pip
& $venvPy -m pip install --quiet -r $reqFile

Write-Host "Running tests/ from $repoRoot" -ForegroundColor Cyan
Push-Location $repoRoot
try {
    & $venvPy -m pytest tests/ @args
    $exitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $exitCode
