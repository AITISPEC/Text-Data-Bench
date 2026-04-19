# force-reinstall.ps1 (расширенная версия)
param(
    [switch]$deps,      # Установить зависимости
    [switch]$verbose,   # Подробный вывод
    [string]$extra=""   # Дополнительные флаги для pip
)

Write-Host "=== Force Reinstall Tool ===" -ForegroundColor Magenta

# Режим подробного вывода
if ($verbose) {
    $pipOutput = ""
    $errorAction = "Continue"
} else {
    $pipOutput = "| Out-Null"
    $errorAction = "SilentlyContinue"
}

Write-Host "[1/5] Clearing Python caches & build artifacts..." -ForegroundColor Cyan
pip cache purge 2>&1 | Out-Null

# Рекурсивная очистка кэша Python
Get-ChildItem -Path . -Filter "__pycache__" -Recurse -Directory -Force | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path . -Filter "*.pyc" -Recurse -File -Force | Remove-Item -Force -ErrorAction SilentlyContinue

# Артефакты сборки
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue build/, dist/, *.egg-info, .pytest_cache, .coverage, htmlcov/

Write-Host "[2/5] Uninstalling old package..." -ForegroundColor Cyan
pip uninstall text-data-bench -y 2>&1 | Out-Null

Write-Host "[3/5] Installing fresh build..." -ForegroundColor Cyan

# Формируем команду pip
$pipCmd = "pip install -e . --no-cache-dir --force-reinstall"

if (-not $deps) {
    $pipCmd += " --no-deps"
    Write-Host "  Mode: WITHOUT dependencies" -ForegroundColor Yellow
} else {
    Write-Host "  Mode: WITH dependencies (from pyproject.toml)" -ForegroundColor Green
}

if ($extra) {
    $pipCmd += " $extra"
    Write-Host "  Extra flags: $extra" -ForegroundColor Cyan
}

# Выполняем установку
if ($verbose) {
    Invoke-Expression $pipCmd
} else {
    Invoke-Expression "$pipCmd 2>&1 | Out-Null"
}

if ($LASTEXITCODE -ne 0) {
    Write-Host " Installation failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[4/5] Verifying installation..." -ForegroundColor Cyan

# Проверка
$importTest = python -c "import text_data_bench; print(' Module imported successfully')" 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "[5/5]  Installation successful." -ForegroundColor Green
    Write-Host $importTest -ForegroundColor Green

    # Показываем установленные зависимости (если нужно)
    if ($deps -and $verbose) {
        Write-Host "`nInstalled dependencies:" -ForegroundColor Cyan
        pip list | Select-String -Pattern "datasets|pandas|numpy|pyarrow|h5py|openpyxl"
    }
} else {
    Write-Host "[5/5]  Installation failed." -ForegroundColor Red
    Write-Host "  Error: $importTest" -ForegroundColor Yellow

    if (-not $deps) {
        Write-Host "`n   Tip: Try running with dependencies:" -ForegroundColor Cyan
        Write-Host "     .\force-reinstall.ps1 -deps" -ForegroundColor White
    }
    exit 1
}
