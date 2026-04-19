# Dump: ROOT FILES

**Root:** `C:\Users\AITISPEC\miniconda3\envs\TextDataBench\py`

---

## 📄 DATA_CARD.md

```md

```

---

## 📄 force-reinstall.ps1

```ps1
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
```

---

## 📄 Makefile

```text
.PHONY: install lint test run clean

install:
	pip install -e ".[dev]"

lint:
	ruff check src tests
	ruff format --check src tests

test:
	pytest -v -m "not slow"

run:
	databench process tests/fixtures/sample.txt output/clean.parquet --config configs/pipeline.yaml

clean:
	rm -rf output/ __pycache__/ .pytest_cache/ build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -r {} +
```

---

## 📄 pyproject.toml

```toml
[project]
name = "text-data-bench"
version = "1.2.0"
description = "Data-centric pipeline with 14 explicit format engines"
requires-python = ">=3.12"
dependencies = [
    "polars>=1.39.3",
    "pydantic>=2.13.2",
    "typer>=0.24.1",
    "rich>=15.0.0",
    "datasketch>=1.10.0",
    "plotly>=6.7.0",
    "pyyaml>=6.0.3",
    "openpyxl>=3.1.5",
    "h5py>=3.16.0",
    "datasets>=4.8.4",
    "xlsxwriter>=3.2.9"
]

[project.scripts]
databench = "text_data_bench.cli:app"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src", "."]
markers = ["slow: marks tests as slow"]
```

---

## 📄 README.md

```md
# 📊 TextDataBench

Автоматизированный конвейер для бенчмаркинга, очистки, дедупликации и балансировки текстовых датасетов. Поддерживает **14 форматов** с детерминированным роутингом и стандартизированным выводом.

## ✨ Ключевые возможности
- **14 движков загрузки**: CSV, TSV, Parquet, Feather, Excel, JSONL, JSON, TXT, MD, XML, HDF5, Arrow, Pickle, HF Datasets.
- **Авто-стандартизация**: Сложные и вложенные структуры автоматически преобразуются в единую колонку `_tdb_text`.
- **Полный пайплайн**: Фильтрация → MinHash-дедупликация → Стратифицированная балансировка → Сбор метрик.
- **Интерактивное CLI**: Удобное меню с поддержкой пакетной обработки (`[0] ALL`).
- **Наглядные отчёты**: Индивидуальные Markdown-файлы для каждого датасета с дельтой метрик «До/После».
- **Безопасный I/O**: Автоматический фоллбэк в `.parquet` при сохранении форматов, не поддерживающих вложенность.

## 🚀 Установка

Требуется **Python 3.12+**. Для корректной работы на Windows рекомендуется использовать `conda` (решает проблемы с бинарными зависимостями `datasets`, `h5py` и др.).

Скрипт `force-reinstall.ps1` поддерживает гибкие режимы установки:

```powershell
# Тихий режим (только ядро, без зависимостей)
.\force-reinstall.ps1

# Полная установка (разрешает все зависимости из pyproject.toml)
.\force-reinstall.ps1 -deps

# Подробный вывод + дополнительные флаги pip
.\force-reinstall.ps1 -deps -verbose -extra "--upgrade-strategy eager"
```

📖 Использование
Запуск интерактивного меню:
```powershell
python -m text_data_bench
```

Пункты меню:
[1] Test — Запуск тестов (pytest -v -m "not slow")
[2] Bench — Обработка датасетов из tests/fixtures/ → сохранение в output/
[3] Force Reinstall — Интерактивный запуск скрипта переустановки
[0] Exit — Выход

💡 Как работать:
Поместите ваши файлы в tests/fixtures/.
Выберите [2] Bench.
Укажите [0] ALL для пакетной обработки или номер конкретного файла.
Готовые .parquet и отчёты <имя>.md появятся в папке output/.

  ⚙️ Конфигурация
Параметры пайплайна управляются через configs/pipeline.yaml:
```yaml
filters:
  min_length: 10      # Минимальная длина текста
  max_length: 10000   # Максимальная длина
dedup:
  fuzzy_threshold: 0.85 # Порог нечёткого дублирования
balance:
  strategy: "stratified" # Стратегия балансировки
  ```

  📦 Поддерживаемые форматы
Категория
Расширения
Табличные
.csv, .tsv, .parquet, .feather, .xlsx, .xls, .jsonl
Текстовые / NLP
.json, .txt, .md, .xml
Специализированные
.h5, .hdf5, .arrow, .pkl, .pickle, .hf

🏗️ Структура проекта
```text
text-data-bench/
├── configs/              # pipeline.yaml, parsers.yaml
├── models/               # Локальные LLM (GGUF)
├── output/               # Результаты обработки и отчёты
├── src/text_data_bench/
│   ├── engines/          # 14 движков загрузки + стандартизатор
│   ├── core/             # Оркестратор пайплайна
│   ├── processors/       # Фильтры, дедуп, балансировка, метрики
│   ├── io/               # Генераторы отчётов, загрузчик конфигов
│   ├── cli.py            # Интерактивное меню
│   └── llm/              # Модуль LLM (готов к интеграции)
├── tests/
│   ├── fixtures/         # Синтетические и реальные датасеты
│   └── generate_fixtures.py # Генератор тестовых данных
└── force-reinstall.ps1   # Скрипт управления зависимостями
```

📄 Лицензия
MIT. Разработан для воспроизводимых процессов инженерии данных и NLP-препроцессинга.
```

---

