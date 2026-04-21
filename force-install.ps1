param(
    [switch]$deps,
    [switch]$verbose,
    [string]$extra = ""
)

Write-Host "=== Force Install Tool ===" -ForegroundColor Magenta

Write-Host "[1/4] Clearing caches..." -ForegroundColor Cyan
pip cache purge 2>&1 | Out-Null
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue __pycache__, *.pyc, build/, dist/, *.egg-info, .pytest_cache

Write-Host "[2/4] Uninstalling old..." -ForegroundColor Cyan
pip uninstall text-data-bench -y 2>&1 | Out-Null

Write-Host "[3/4] Installing text-data-bench..." -ForegroundColor Cyan
$pipArgs = @("install", "--no-cache-dir", "--force-reinstall")
if (-not $deps) {
    $pipArgs += "-e", ".", "--no-deps"
    Write-Host "  Mode: WITHOUT dependencies" -ForegroundColor Yellow
}
else {
    $pipArgs += "-e", "."
    Write-Host "  Mode: WITH dependencies" -ForegroundColor Green
}
if ($extra) { $pipArgs += $extra.Split(" ") }

if ($verbose) {
    Write-Host "  Running: pip $pipArgs" -ForegroundColor Gray
    & pip @pipArgs
} else {
    & pip @pipArgs 2>&1 | Out-Null
}
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Failed to install text-data-bench" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[4/4] Verifying..." -ForegroundColor Cyan
$imp = python -c "import text_data_bench; print(' OK: module loaded')" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host $imp -ForegroundColor Green

    Write-Host "`n=== Installation Successful ===" -ForegroundColor Green
    if ($deps -and $verbose) {
        pip list | Select-String -Pattern "datasets|pandas|numpy|pyarrow|h5py|openpyxl"
    }
} else {
    Write-Host "Verification failed: $imp" -ForegroundColor Red
    exit 1
}
