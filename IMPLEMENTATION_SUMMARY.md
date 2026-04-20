# Резюме изменений: Поддержка GGUF моделей для ускорения вычислений

## Обзор

В ответ на запрос о возможности использования файла `./models/Qwen2-500M-Instruct-Q8_0.gguf` для ускорения вычислений без установки PyTorch+CUDA, были реализованы следующие изменения:

---

## ✅ Реализованные изменения

### 1. **Модуль обнаружения устройств** (`src/text_data_bench/core/device.py`)

#### Новые функции:
- `detect_gpu()` - Обнаружение NVIDIA GPU через nvidia-smi
- `detect_mps()` - Обнаружение Apple Silicon MPS (Metal)
- `validate_model_path()` - Валидация пути к GGUF модели
- `get_llm_gpu_layers()` - Определение количества слоёв для GPU с поддержкой CUDA и MPS

#### Преимущества:
- Автоматическое переключение между CPU, NVIDIA CUDA и Apple Metal
- Логирование всех событий детектирования
- Graceful degradation при отсутствии GPU

---

### 2. **LLM движок** (`src/text_data_bench/llm/engine.py`)

#### Изменения:
- Функция `get_engine()` теперь возвращает `None` если модель не найдена или llama-cpp-python не установлен
- Добавлена обработка исключений с fallback на CPU
- Stub-режим при отсутствии llama-cpp-python
- Авто-детект числа CPU потоков

#### API:
```python
from text_data_bench.llm import get_engine

model = get_engine(
    model_path="./models/Qwen2-500M-Instruct-Q8_0.gguf",
    ctx=512,
    prefer_gpu=True
)
```

---

### 3. **Конфигурация** (`configs/pipeline.yaml`)

#### Новые параметры:
```yaml
pipeline:
  model_path: "./models/Qwen2-500M-Instruct-Q8_0.gguf"
  llm_context: 512
  prefer_gpu: true
```

---

### 4. **Схема конфигурации** (`src/text_data_bench/config/base.py`)

#### Обновлённый класс `PipelineCfg`:
```python
class PipelineCfg(BaseModel):
    prefer_gpu: bool = True
    text_col: Optional[str] = None
    model_path: Optional[str] = "./models/Qwen2-500M-Instruct-Q8_0.gguf"
    llm_context: int = 512
```

---

### 5. **Зависимости** (`pyproject.toml`)

#### Добавлено:
```toml
dependencies = [
    ...
    "llama-cpp-python>=0.2.0"
]

[project.optional-dependencies]
gpu = [
    "torch>=2.0.0",
    "torchvision>=0.15.0"
]
```

---

### 6. **Документация**

#### Обновлённый README.md:
- Секция об установке GGUF моделей
- Примеры использования
- Таблица поддерживаемых платформ

#### Новый файл MODEL_USAGE.md:
- Полное руководство по использованию GGUF моделей
- Инструкции по установке для разных платформ
- Рекомендуемые модели и их характеристики
- Диагностика проблем
- Бенчмарки производительности

---

### 7. **Структура проекта**

#### Создана папка для моделей:
```
models/
├── .gitkeep          # Пустой файл для отслеживания в git
└── .gguf             # Игнорируется через .gitignore
```

---

## 🔧 Как использовать

### Шаг 1: Установка llama-cpp-python

**CPU (базовая):**
```bash
pip install llama-cpp-python
```

**NVIDIA GPU:**
```bash
CMAKE_ARGS="-DGGML_CUDA=on" pip install llama-cpp-python --force-reinstall --no-cache-dir
```

**Apple MPS:**
```bash
pip install llama-cpp-python
# MPS активируется автоматически при наличии torch
```

### Шаг 2: Скачивание модели

```bash
mkdir -p models

# Пример для Qwen2 500M
wget https://huggingface.co/Qwen/Qwen2-0.5B-Instruct-GGUF/resolve/main/qwen2-0_5b-instruct-q8_0.gguf \
     -O models/Qwen2-500M-Instruct-Q8_0.gguf
```

### Шаг 3: Настройка конфигурации

Отредактируйте `configs/pipeline.yaml`:
```yaml
pipeline:
  prefer_gpu: true
  model_path: "./models/Qwen2-500M-Instruct-Q8_0.gguf"
  llm_context: 512
```

### Шаг 4: Запуск пайплайна

```bash
python -m text_data_bench
# Выбрать [2] Bench для обработки датасетов
```

---

## 📊 Поддерживаемые платформы

| Платформа | Ускорение | Требования | Примечания |
|-----------|-----------|------------|------------|
| **CPU** | ✅ | Нет | Работает везде, авто-детект потоков |
| **NVIDIA GPU** | ✅✅✅ | nvidia-smi, драйверы | CUDA через CMAKE_ARGS |
| **Apple Silicon** | ✅✅ | torch (опционально) | MPS автоматически |

---

## 🎯 Преимущества решения

1. **Без PyTorch**: Не требует установки тяжёлого PyTorch+CUDA
2. **Кроссплатформенность**: Работает на Windows, Linux, macOS
3. **Грамотный fallback**: Автоматический откат на CPU при проблемах
4. **Опциональность**: Проект работает и без llama-cpp-python
5. **Лёгкая установка**: Один пакет вместо сложной настройки CUDA

---

## 📈 Ожидаемая производительность

| Платформа | Токенов/сек | Использование памяти |
|-----------|-------------|---------------------|
| CPU (8 ядер) | 10-20 | ~1GB |
| NVIDIA RTX 3060 | 50-100 | ~1GB |
| Apple M2 | 30-60 | ~1GB |

---

## 📝 Список изменённых файлов

```
Modified:
  - README.md
  - configs/pipeline.yaml
  - pyproject.toml
  - src/text_data_bench/config/base.py
  - src/text_data_bench/core/__init__.py
  - src/text_data_bench/core/device.py
  - src/text_data_bench/llm/__init__.py
  - src/text_data_bench/llm/engine.py

Added:
  - MODEL_USAGE.md
  - models/.gitkeep
```

---

## ⚠️ Важные замечания

1. **Файл модели игнорируется git**: `.gitignore` содержит правило `models/*.gguf`
2. **Путь по умолчанию**: Конфигурация указывает на `./models/Qwen2-500M-Instruct-Q8_0.gguf`, но файл нужно скачать отдельно
3. **Обратная совместимость**: Все изменения обратно совместимы - проект работает без модели и без llama-cpp-python

---

## 🔮 Планы на будущее

- Интеграция LLM в пайплайн обработки данных
- Поддержка batch-инференса
- Кэширование результатов инференса
- Дополнительные форматы квантования (Q4_K_M, Q5_K_M, etc.)
