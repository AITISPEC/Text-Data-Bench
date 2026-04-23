# 📊 TextDataBench

Автоматизированный конвейер для бенчмаркинга, очистки, дедупликации и балансировки текстовых и табличных датасетов. Поддерживает **17 форматов** с детерминированным роутингом и стандартизированным выводом.

## ✨ Ключевые возможности

- **17 движков загрузки**: CSV, TSV, Parquet, Feather, Excel, JSONL, JSON, TXT, MD, XML, HDF5, Arrow, Pickle, HF Datasets, NPY, NPZ, а также графовые форматы GML, GraphML, DOT
- **Авто-стандартизация**: Любые данные (текст, числа, графы) автоматически преобразуются в единую колонку `_tdb_text`
- **Полный пайплайн**: Фильтрация → MinHash-дедупликация → Стратифицированная балансировка → Сбор метрик
- **Интерактивное CLI**: Удобное меню с поддержкой пакетной обработки (`[0] ALL`)
- **Наглядные отчёты**: Индивидуальные Markdown-файлы для каждого датасета с дельтой метрик «До/После»
- **Безопасный I/O**: Автоматический фоллбэк в `.parquet` при сохранении форматов, не поддерживающих вложенность
- **GPU-детекция**: Автоматическое определение NVIDIA GPU при включённой опции `prefer_gpu`

## 📦 Поддерживаемые форматы

| № | Категория | Формат | Расширения |
|---|-----------|--------|------------|
| 1 | Табличные | CSV | `.csv` |
| 2 | Табличные | TSV | `.tsv` |
| 3 | Табличные | Parquet | `.parquet` |
| 4 | Табличные | Feather | `.feather` |
| 5 | Табличные | Excel | `.xlsx`, `.xls` |
| 6 | Табличные | JSONL | `.jsonl` |
| 7 | Табличные | JSON | `.json` |
| 8 | Текстовые | Plain Text | `.txt` |
| 9 | Текстовые | Markdown | `.md` |
| 10 | Текстовые | XML | `.xml` |
| 11 | Графовые | GML | `.gml` |
| 12 | Графовые | GraphML | `.graphml` |
| 13 | Графовые | DOT | `.dot` |
| 14 | Специализированные | HDF5 | `.h5`, `.hdf5` |
| 15 | Специализированные | Arrow IPC | `.arrow` |
| 16 | Специализированные | Pickle | `.pkl`, `.pickle` |
| 17 | Специализированные | Hugging Face | `.hf` |
| + | NumPy | NPY / NPZ | `.npy`, `.npz` |

## 🚀 Установка

### Windows (рекомендуется)

```powershell
git clone https://github.com/AITISPEC/Text-Data-Bench.git
cd Text-Data-Bench
.\force-install.ps1 -create_env -verbose
```

### Параметры force-install.ps1:

```text
-create_env – создать виртуальное окружение

-gpu – установка с поддержкой CUDA

-verbose – подробный вывод

-nodeps – не устанавливать зависимости
```

### Linux / macOS

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 📖 Использование

Активация окружения
```powershell
# Windows
.\.venv\Scripts\Activate.ps1

# Linux/Mac
source .venv/bin/activate
```
Запуск интерактивного меню:
```powershell
python -m text_data_bench
```

Пункты меню:
- `[1] Test` — Запуск тестов (pytest -v -m "not slow")
- `[2] Bench` — Обработка датасетов из `tests/fixtures/` → сохранение в `output/`
- `[0] Exit` — Выход

💡 **Как работать:**
1. Поместите ваши файлы в `tests/fixtures/`.
2. Выберите `[1] Test` затем `[2] Bench`.
3. Укажите `[0] ALL` для пакетной обработки или номер конкретного файла.
4. Готовые `.parquet` и отчёты `<имя>.md` появятся в папке `output/`.

## ⚙️ Конфигурация

Пример configs/pipeline.yaml:
```yaml
pipeline:
  prefer_gpu: false
filters:
  min_length: 10
  max_length: 10000
  remove_empty: true
dedup:
  exact: true
  fuzzy: true
  fuzzy_threshold: 0.85
  num_perm: 128
balance:
  strategy: "stratified"
  group_col: null
  seed: 42
output:
  format: "parquet"
  report_path: "output/report.md"
logging:
  level: "INFO"
  json_format: false
```

## 🏗️ Структура проекта

```text
text-data-bench/
├── configs/              # pipeline.yaml
├── output/               # Результаты обработки
├── src/text_data_bench/
│   ├── engines/          # 17 движков загрузки
│   ├── core/             # Пайплайн, GPU-детекция
│   ├── processors/       # Фильтры, дедуп, балансировка
│   ├── io/               # Генерация отчётов
│   ├── utils/            # Логгер, кэш
│   └── cli.py
├── tests/                # Тесты и фикстуры
└── force-install.ps1     # Только для Windows
```
