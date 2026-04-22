param(
    [switch]$deps,
    [switch]$verbose,
    [switch]$gpu,
    [switch]$create_env,
    [string]$env_name = ".venv",
    [string]$extra = ""
)

Write-Host "=== Force Install Tool ===" -ForegroundColor Magenta

# ==================== ФУНКЦИИ ====================
function Test-Python312 {
    try {
        $result = & python --version 2>&1
        if ($result -match "Python (\d+)\.(\d+)") {
            $major = [int]$matches[1]
            $minor = [int]$matches[2]
            return ($major -eq 3 -and $minor -ge 12)
        }
    } catch {
        return $false
    }
    return $false
}

function Install-PythonWindows {
    Write-Host "  Downloading Python 3.12 installer..." -ForegroundColor Gray
    $pythonUrl = "https://www.python.org/ftp/python/3.12.4/python-3.12.4-amd64.exe"
    $installer = "$env:TEMP\python-3.12.4-amd64.exe"

    try {
        $webClient = New-Object System.Net.WebClient
        $webClient.DownloadFile($pythonUrl, $installer)
        Write-Host "  ✅ Downloaded" -ForegroundColor Green

        Write-Host "  Installing Python 3.12 (this may take a minute)..." -ForegroundColor Gray
        $process = Start-Process -FilePath $installer -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0" -Wait -PassThru
        if ($process.ExitCode -eq 0) {
            Write-Host "  ✅ Python 3.12 installed successfully" -ForegroundColor Green
            $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
            return $true
        } else {
            Write-Host "  ❌ Installation failed with code: $($process.ExitCode)" -ForegroundColor Red
            return $false
        }
    } catch {
        Write-Host "  ❌ Download failed: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    } finally {
        Remove-Item $installer -ErrorAction SilentlyContinue
    }
}

function Install-PythonLinux {
    Write-Host "  Detected Linux. Please install Python 3.12 manually:" -ForegroundColor Yellow
    Write-Host "    sudo add-apt-repository ppa:deadsnakes/ppa" -ForegroundColor Gray
    Write-Host "    sudo apt update" -ForegroundColor Gray
    Write-Host "    sudo apt install python3.12 python3.12-venv python3.12-dev" -ForegroundColor Gray
    return $false
}

function Install-PythonMac {
    Write-Host "  Detected macOS. Please install Python 3.12 manually:" -ForegroundColor Yellow
    Write-Host "    brew install python@3.12" -ForegroundColor Gray
    Write-Host "  Or download from https://www.python.org/downloads/" -ForegroundColor Gray
    return $false
}

# ==================== ПУНКТ 1/6: ПРОВЕРКА И УСТАНОВКА PYTHON ====================
Write-Host "[1/6] Checking Python 3.12+..." -ForegroundColor Cyan

$hasPython312 = Test-Python312

if (-not $hasPython312) {
    Write-Host "  ⚠️ Python 3.12+ not found" -ForegroundColor Yellow

    if ($create_env) {
        Write-Host "  Attempting to install Python 3.12..." -ForegroundColor Cyan

        $os = $env:OS
        if ($os -like "*Windows*") {
            $installed = Install-PythonWindows
        } elseif ($os -like "*Linux*") {
            $installed = Install-PythonLinux
        } elseif ($os -like "*Darwin*") {
            $installed = Install-PythonMac
        } else {
            Write-Host "  ❌ Unsupported OS for automatic installation" -ForegroundColor Red
            $installed = $false
        }

        if (-not $installed) {
            Write-Host "  ❌ Could not install Python 3.12 automatically" -ForegroundColor Red
            Write-Host "  Please install Python 3.12+ manually from https://python.org" -ForegroundColor Yellow
            exit 1
        }

        $hasPython312 = Test-Python312
        if (-not $hasPython312) {
            Write-Host "  ❌ Python installation verification failed" -ForegroundColor Red
            Write-Host "  Please restart your terminal and run this script again" -ForegroundColor Yellow
            exit 1
        }
    } else {
        Write-Host "  Please install Python 3.12+ or use -create_env flag" -ForegroundColor Yellow
        Write-Host "  Example: .\force-install.ps1 -deps -create_env" -ForegroundColor Gray
        Write-Host ""
        Write-Host "  Download Python: https://www.python.org/downloads/" -ForegroundColor Cyan
        exit 1
    }
}

Write-Host "  ✅ Python 3.12+ available" -ForegroundColor Green
$pythonVersion = & python --version 2>&1
Write-Host "  $pythonVersion" -ForegroundColor Gray

# ==================== ПУНКТ 2/6: СОЗДАНИЕ VENV ====================
Write-Host "[2/6] Setting up venv environment..." -ForegroundColor Cyan

$venvPath = ".\$env_name"

if ($create_env) {
    Write-Host "  Creating venv environment '$env_name'..." -ForegroundColor Gray

    if (Test-Path $venvPath) {
        Write-Host "  Environment '$env_name' already exists, removing..." -ForegroundColor Gray
        Remove-Item -Recurse -Force $venvPath
    }

    & python -m venv $env_name

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ❌ Failed to create venv" -ForegroundColor Red
        exit 1
    }

    Write-Host "  ✅ venv created: $venvPath" -ForegroundColor Green

    $activateScript = "$venvPath\Scripts\Activate.ps1"
    if (Test-Path $activateScript) {
        & $activateScript
        $env:PATH = "$venvPath\Scripts;$env:PATH"
        Write-Host "  ✅ Environment activated" -ForegroundColor Green
    }
} else {
    Write-Host "  Using existing Python environment" -ForegroundColor Gray
}

# ==================== ПУНКТ 3/6: ПРОВЕРКА ПОДКЛЮЧЕНИЯ ====================
Write-Host "[3/6] Checking network connectivity..." -ForegroundColor Cyan

function Test-PortOpen {
    param([string]$Hostname, [int]$Port = 443)
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $asyncResult = $tcpClient.BeginConnect($Hostname, $Port, $null, $null)
        $wait = $asyncResult.AsyncWaitHandle.WaitOne(3000)
        if ($wait) {
            $tcpClient.EndConnect($asyncResult)
            $tcpClient.Close()
            return $true
        }
        $tcpClient.Close()
        return $false
    } catch {
        return $false
    }
}

function Get-Latency {
    param([string]$Hostname)
    try {
        $ping = New-Object System.Net.NetworkInformation.Ping
        $reply = $ping.Send($Hostname, 2000)
        if ($reply.Status -eq "Success") {
            return "$($reply.RoundtripTime)ms"
        }
        return "N/A"
    } catch {
        return "N/A"
    }
}

$servers = @(
    @{Name = "PyPI"; Host = "pypi.python.org"}
    @{Name = "Python Hosting"; Host = "files.pythonhosted.org"}
    @{Name = "Hugging Face"; Host = "huggingface.co"}
    @{Name = "GitHub"; Host = "github.com"}
    @{Name = "NVIDIA"; Host = "pypi.nvidia.com"}
)

$allReachable = $true
$results = @()

foreach ($server in $servers) {
    Write-Host "  Checking $($server.Name)..." -NoNewline -ForegroundColor Gray

    $portOpen = Test-PortOpen -Hostname $server.Host -Port 443

    if ($portOpen) {
        $latency = Get-Latency -Hostname $server.Host
        Write-Host " ✅ OK" -ForegroundColor Green
        $results += [PSCustomObject]@{Server = $server.Name; Status = "OK"; Latency = $latency }
    }
    else {
        Write-Host " ❌ FAILED" -ForegroundColor Red
        $results += [PSCustomObject]@{Server = $server.Name; Status = "Unreachable"; Latency = "N/A" }
        $allReachable = $false
    }
}

Write-Host "`n  Testing download speed..." -ForegroundColor Gray
try {
    $speedTestUrl = "https://files.pythonhosted.org/packages/source/p/pip/pip-24.0.tar.gz"
    $webClient = New-Object System.Net.WebClient
    $startTime = Get-Date
    $data = $webClient.DownloadData($speedTestUrl)
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    $sizeMB = $data.Length / 1MB
    $speedMbps = ($sizeMB * 8) / $duration

    Write-Host "  ✅ Download speed: $([math]::Round($speedMbps, 2)) Mbps" -ForegroundColor Green
} catch {
    Write-Host "  ⚠️ Speed test failed: $($_.Exception.Message)" -ForegroundColor Yellow
}

Write-Host "`n  Summary:" -ForegroundColor Cyan
$results | Format-Table -AutoSize

if (-not $allReachable) {
    Write-Host "  ⚠️ Some servers are unreachable. Installation may be slow." -ForegroundColor Yellow
    $continue = Read-Host "  Continue anyway? (y/n)"
    if ($continue -ne 'y') {
        Write-Host "  Aborted." -ForegroundColor Red
        exit 1
    }
}

# ==================== ПУНКТ 4/6: ОЧИСТКА КЭША ====================
Write-Host "[4/6] Clearing caches..." -ForegroundColor Cyan
pip cache purge 2>&1 | Out-Null
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue __pycache__, *.pyc, build/, dist/, *.egg-info, .pytest_cache

# ==================== ПУНКТ 5/6: УСТАНОВКА ПРОЕКТА ====================
Write-Host "[5/6] Installing text-data-bench..." -ForegroundColor Cyan
python -m pip install --upgrade pip
$pipArgs = @("install", "--no-cache-dir", "--force-reinstall")

# 1. Определяем цель установки (с экстрой или без)
$installTarget = "."
if ($gpu) {
    $installTarget = ".[gpu]"
    $pipArgs += "--extra-index-url", "https://pypi.nvidia.com"
    Write-Host "  GPU mode active: Added NVIDIA index" -ForegroundColor Magenta
}

# 2. Логика установки зависимостей
if (-not $deps) {
    $pipArgs += "-e", $installTarget, "--no-deps"
    Write-Host "  Mode: WITHOUT dependencies (editable)" -ForegroundColor Yellow
}
else {
    $pipArgs += "-e", $installTarget
    Write-Host "  Mode: WITH dependencies (editable)" -ForegroundColor Green
}

# 3. Добавляем $extra аргументы, если они есть
if ($extra) {
    $pipArgs += $extra.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries)
}

# 4. Выполнение команды
if ($verbose) {
    Write-Host "  Running: pip $($pipArgs -join ' ')" -ForegroundColor Gray
    & pip @pipArgs
} else {
    Write-Host "  Installing... (please wait)" -ForegroundColor Gray
    & pip @pipArgs 2>&1 | Out-Null
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "  ❌ Failed to install project" -ForegroundColor Red
    exit $LASTEXITCODE
}
Write-Host "  ✅ Project installed successfully" -ForegroundColor Green

# ==================== ПУНКТ 6/6: ПРОВЕРКА GPU (если выбран) ====================
if ($gpu) {
    Write-Host "[6/6] Verifying GPU support..." -ForegroundColor Cyan
    $checkCode = 'import polars as pl; print("Polars GPU check:", end=" "); ' + `
                 'try: pl.LazyFrame({"a":[1]}).collect(engine="cudf"); print("✅ SUCCESS") ' + `
                 'except Exception as e: print(f"❌ FAILED ({e})")'
    & python -c $checkCode
}

# ==================== ФИНАЛЬНАЯ ПРОВЕРКА ====================
$imp = python -c "import text_data_bench; print(' OK: module loaded')" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host $imp -ForegroundColor Green
    Write-Host "`n=== Installation Successful ===" -ForegroundColor Green
    if ($create_env) {
        Write-Host "  Environment: $venvPath" -ForegroundColor Cyan
        Write-Host "  To activate manually: .\$env_name\Scripts\Activate.ps1" -ForegroundColor Gray
    }
    if ($deps -and $verbose) {
        pip list | Select-String -Pattern "datasets|pandas|numpy|pyarrow|h5py|openpyxl|networkx|pydot"
    }
} else {
    Write-Host "Verification failed: $imp" -ForegroundColor Red
    exit 1
}

Write-Host "`n=== Setup Complete! ===" -ForegroundColor Magenta
