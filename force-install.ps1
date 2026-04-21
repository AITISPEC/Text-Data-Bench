param(
    [switch]$deps,
    [switch]$verbose,
    [switch]$gpu,
    [string]$extra = ""
)

Write-Host "=== Force Install Tool ===" -ForegroundColor Magenta

Write-Host "[1/5] Clearing caches..." -ForegroundColor Cyan
pip cache purge 2>&1 | Out-Null
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue __pycache__, *.pyc, build/, dist/, *.egg-info, .pytest_cache

Write-Host "[2/5] Uninstalling old..." -ForegroundColor Cyan
pip uninstall text-data-bench -y 2>&1 | Out-Null

Write-Host "[3/5] Installing PyTorch..." -ForegroundColor Cyan
if ($gpu) {
    $index = "https://download.pytorch.org/whl/cu128"
    Write-Host "  Installing: PyTorch with CUDA 12.8" -ForegroundColor Gray
} else {
    $index = "https://download.pytorch.org/whl/cpu"
    Write-Host "  Installing: PyTorch CPU version" -ForegroundColor Yellow
}

pip install torch torchvision torchaudio --index-url $index --no-cache-dir --force-reinstall

if ($LASTEXITCODE -ne 0) {
    Write-Host "  Failed to install PyTorch" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[4/5] Installing text-data-bench..." -ForegroundColor Cyan
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

Write-Host "[5/5] Verifying..." -ForegroundColor Cyan
$imp = python -c "import text_data_bench; print(' OK: module loaded')" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host $imp -ForegroundColor Green

    Write-Host "`nChecking GPU..." -ForegroundColor Cyan
    $chk = Join-Path $PSScriptRoot "check_cuda.ps1"
    if (Test-Path $chk) {
        $res = powershell -ExecutionPolicy Bypass -File $chk 2>&1
        if ($res -match "ENABLED" -or $res -match "YES") {
            Write-Host $res -ForegroundColor Green
        } else {
            Write-Host $res -ForegroundColor Yellow
        }
    } else {
        Write-Host "  [WARN] check_cuda.ps1 not found. Skipping GPU check." -ForegroundColor Yellow
    }

    Write-Host "`n=== Installation Successful ===" -ForegroundColor Green
    if ($deps -and $verbose) {
        pip list | Select-String -Pattern "datasets|pandas|numpy|pyarrow|h5py|openpyxl|llama-cpp"
    }
} else {
    Write-Host "Verification failed: $imp" -ForegroundColor Red
    exit 1
}
