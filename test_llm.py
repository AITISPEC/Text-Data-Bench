# test_llm.py
from text_data_bench.llm.engine import get_engine
from pathlib import Path

# Явно указываем путь относительно текущего файла или используем абсолютный
# Но так как вы в корне, "./models" должно работать, если запускать из этой папки
model_path = "./models/Qwen2-500M-Instruct-Q8_0.gguf"

#print(f"Checking path: {model_path.resolve()}")
#print(f"Files in dir: {list(model_path.iterdir())}")

engine = get_engine(model_path)

if hasattr(engine, 'status'):
	print("Engine status:", engine.status)
else:
	print("Engine status: OK (Stub or Loaded)")

# Попробуем простой инференс, если движок активен
if hasattr(engine, 'generate'):
	try:
		result = engine.generate("Привет", max_tokens=5)
		print(f"Generation test: {result}")
	except Exception as e:
		print(f"Generation failed: {e}")
