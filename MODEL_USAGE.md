# Использование GGUF моделей в TextDataBench

## Обзор

TextDataBench поддерживает использование локальных GGUF-моделей через библиотеку `llama-cpp-python`. Это позволяет:
- Использовать LLM для расширенной обработки текстов
- Получать ускорение на GPU без установки PyTorch+CUDA
- Работать с квантованными моделями для экономии памяти

## Поддерживаемые платформы

| Платформа | Ускорение | Примечания |
|-----------|-----------|------------|
| **CPU** | ✅ | Работает везде, автоматически определяет количество потоков |
| **NVIDIA GPU** | ✅✅✅ | Требуется `nvidia-smi`, автоматическое переключение на CUDA |
| **Apple Silicon** | ✅✅ | MPS (Metal Performance Shaders) через torch |

## Установка

### Базовая установка (CPU)
```bash
pip install llama-cpp-python
```

### С поддержкой NVIDIA GPU
```bash
CMAKE_ARGS="-DGGML_CUDA=on" pip install llama-cpp-python --force-reinstall --no-cache-dir
```

### С поддержкой Apple MPS
```bash
pip install llama-cpp-python
# MPS активируется автоматически при наличии torch
```

## Настройка модели

### 1. Скачивание модели

Поместите GGUF модель в папку `models/`:

```bash
mkdir -p models

# Пример: Qwen2 500M Instruct (квантование Q8_0)
wget https://huggingface.co/Qwen/Qwen2-0.5B-Instruct-GGUF/resolve/main/qwen2-0_5b-instruct-q8_0.gguf \
     -O models/Qwen2-500M-Instruct-Q8_0.gguf

# Или используйте huggingface-cli
huggingface-cli download Qwen/Qwen2-0.5B-Instruct-GGUF \
    qwen2-0_5b-instruct-q8_0.gguf \
    --local-dir models \
    --local-dir-use-symlinks false
```

### 2. Конфигурация

Отредактируйте `configs/pipeline.yaml`:

```yaml
pipeline:
  prefer_gpu: true                    # Включить авто-детект GPU
  model_path: "./models/Qwen2-500M-Instruct-Q8_0.gguf"
  llm_context: 512                    # Размер контекстного окна
```

### 3. Проверка доступности модели

```python
from text_data_bench.core.device import validate_model_path

if validate_model_path("./models/Qwen2-500M-Instruct-Q8_0.gguf"):
    print("✓ Модель готова к использованию")
else:
    print("⚠ Модель не найдена или путь неверен")
```

## Рекомендуемые модели

| Модель | Размер | Квантование | RAM | Использование |
|--------|--------|-------------|-----|---------------|
| Qwen2-0.5B-Instruct | ~500MB | Q8_0 | 1GB | Быстрая обработка, классификация |
| Qwen2-0.5B-Instruct | ~300MB | Q4_K_M | 600MB | Экономия памяти |
| Phi-2 | ~2GB | Q8_0 | 4GB | Более сложные задачи |
| Mistral-7B | ~5GB | Q4_K_M | 8GB | Продвинутый анализ |

## Программное использование

```python
from text_data_bench.llm.engine import get_engine

# Загрузка модели с авто-детектом GPU
model = get_engine(
    model_path="./models/Qwen2-500M-Instruct-Q8_0.gguf",
    ctx=512,
    prefer_gpu=True
)

if model is None:
    print("Модель не загружена, продолжаем без LLM")
else:
    # Использование модели
    output = model(
        "Обработай этот текст: ...",
        max_tokens=100,
        stop=["</s>"]
    )
```

## Диагностика проблем

### Модель не загружается

```python
from text_data_bench.core.device import detect_gpu, detect_mps, validate_model_path

print(f"GPU доступен: {detect_gpu()}")
print(f"MPS доступен: {detect_mps()}")
print(f"Модель существует: {validate_model_path('./models/model.gguf')}")
```

### Принудительное использование CPU

Если возникают проблемы с GPU, установите:

```yaml
pipeline:
  prefer_gpu: false  # Всегда использовать CPU
```

### Логи загрузки

При загрузке модели вы увидите:
- `[blue]📦 Loading GGUF: ...` - Начало загрузки
- `[green]✓ GPU detected. Routing LLM to CUDA.` - Успешное обнаружение GPU
- `[yellow]⚠ GPU unavailable. Falling back to CPU.` - Откат на CPU
- `[red]❌ Failed to load model: ...` - Ошибка загрузки

## Производительность

| Платформа | Токенов/сек | Задержка |
|-----------|-------------|----------|
| CPU (8 ядер) | 10-20 | ~50ms |
| NVIDIA RTX 3060 | 50-100 | ~10ms |
| Apple M2 | 30-60 | ~20ms |

## Дополнительные ресурсы

- [llama-cpp-python документация](https://github.com/abetlen/llama-cpp-python)
- [Hugging Face GGUF модели](https://huggingface.co/models?library=gguf)
- [TheBloke квантованные модели](https://huggingface.co/TheBloke)
