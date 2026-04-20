#!/usr/bin/env python3
"""
Unified GPU and LLM Diagnostic Tool
Проверяет CUDA, llama-cpp-python и тестирует инференс модели
"""

import sys
import os
import ctypes
from pathlib import Path

# ANSI цвета для красивого вывода (опционально)
class Colors:
	HEADER = '\033[95m'
	BLUE = '\033[94m'
	CYAN = '\033[96m'
	GREEN = '\033[92m'
	YELLOW = '\033[93m'
	RED = '\033[91m'
	END = '\033[0m'
	BOLD = '\033[1m'

def print_header(text):
	print(f"\n{Colors.HEADER}{'='*60}{Colors.END}")
	print(f"{Colors.BOLD}{Colors.CYAN}{text:^60}{Colors.END}")
	print(f"{Colors.HEADER}{'='*60}{Colors.END}\n")

def print_success(text):
	print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_error(text):
	print(f"{Colors.RED}✗ {text}{Colors.END}")

def print_warning(text):
	print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")

def print_info(text):
	print(f"{Colors.BLUE}ℹ {text}{Colors.END}")

def check_python_env():
	"""Проверка окружения Python"""
	print_header("1. PYTHON ENVIRONMENT")
	print_info(f"Python version: {sys.version}")
	print_info(f"Python executable: {sys.executable}")
	print_info(f"Platform: {sys.platform}")

	import site
	print_info(f"Site packages: {site.getsitepackages()[0]}")

	# Проверка виртуального окружения
	if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
		print_success(f"Virtual environment: {sys.prefix}")
	else:
		print_warning("Not running in virtual environment")

def check_cuda_runtime():
	"""Проверка CUDA Runtime"""
	print_header("2. CUDA RUNTIME")

	# Проверка переменных окружения
	cuda_path = os.environ.get('CUDA_PATH', 'NOT SET')
	print_info(f"CUDA_PATH: {cuda_path}")

	# Проверка DLL
	dll_names = ['cudart64_12.dll', 'cudart64_11.dll', 'cudart64_120.dll', 'cudart64_110.dll']
	cuda_found = False

	for dll in dll_names:
		try:
			ctypes.CDLL(dll)
			print_success(f"CUDA Runtime: YES ({dll})")
			cuda_found = True
			break
		except OSError:
			continue
		except Exception as e:
			print_warning(f"Error checking {dll}: {e}")

	if not cuda_found:
		print_error("CUDA Runtime: NOT FOUND")
		print_info("Make sure CUDA Toolkit is installed and in PATH")

		# Проверка стандартных путей
		standard_paths = [
			r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.4\bin",
			r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v12.1\bin",
			r"C:\Program Files\NVIDIA GPU Computing Toolkit\CUDA\v11.8\bin",
		]

		for path in standard_paths:
			for dll in dll_names:
				if os.path.exists(os.path.join(path, dll)):
					print_info(f"Found {dll} at: {path}")
					print_info(f"Add to PATH or set CUDA_PATH={os.path.dirname(path)}")

	return cuda_found

def check_llama_cpp():
	"""Проверка установки llama-cpp-python"""
	print_header("3. LLAMA-CPP-PYTHON")

	try:
		import llama_cpp
		print_success(f"Version: {llama_cpp.__version__}")
		print_info(f"Installation path: {llama_cpp.__file__}")
	except ImportError as e:
		print_error(f"llama-cpp-python NOT installed: {e}")
		return False
	except Exception as e:
		print_error(f"Unexpected error: {type(e).__name__}: {e}")
		return False

	# Проверка CUDA поддержки
	try:
		from llama_cpp import Llama

		# Разные способы проверки CUDA
		cuda_indicators = []

		if hasattr(Llama, 'cuda'):
			cuda_indicators.append("Llama.cuda attribute")

		if hasattr(Llama, 'supports_gpu_offload'):
			cuda_indicators.append("Llama.supports_gpu_offload")

		# Проверка через атрибуты модуля
		import llama_cpp.llama_cpp as lib
		if hasattr(lib, 'llama_backend_cuda'):
			cuda_indicators.append("llama_backend_cuda function")

		# Проверка через доступные методы
		if hasattr(lib, 'llama_cuda_transform_tensor'):
			cuda_indicators.append("llama_cuda_transform_tensor function")

		if cuda_indicators:
			print_success(f"CUDA compiled: YES")
			for indicator in cuda_indicators:
				print_info(f"  - Found: {indicator}")
		else:
			print_warning("CUDA compiled: NO (CPU only)")
			print_info("Installed version doesn't have CUDA support")

			# Проверка, какой wheel установлен
			import subprocess
			result = subprocess.run([sys.executable, '-m', 'pip', 'show', 'llama-cpp-python'],
								  capture_output=True, text=True)
			if result.returncode == 0:
				for line in result.stdout.split('\n'):
					if line.startswith('Location:'):
						loc = line.split(':', 1)[1].strip()
						print_info(f"Package location: {loc}")
						# Проверяем наличие .pyd файлов с cuda в имени
						for f in os.listdir(loc):
							if f.endswith('.pyd') and 'cuda' in f.lower():
								print_success(f"Found CUDA module: {f}")

	except Exception as e:
		print_error(f"CUDA check failed: {e}")

	return True

def test_model_inference(model_path):
	"""Тестирование инференса модели"""
	print_header("4. MODEL INFERENCE TEST")

	# Проверка существования файла
	model_file = Path(model_path)
	if not model_file.exists():
		print_error(f"Model not found: {model_path}")
		print_info("Looking in current directory:")
		for f in Path('.').glob('*.gguf'):
			print_info(f"  - {f}")
		return False

	print_success(f"Model found: {model_file}")
	print_info(f"Model size: {model_file.stat().st_size / (1024**3):.2f} GB")

	try:
		from text_data_bench.llm.engine import get_engine

		print_info("Creating engine...")
		engine = get_engine(str(model_file))

		# Проверка статуса
		if hasattr(engine, 'status'):
			print_info(f"Engine status: {engine.status}")
		else:
			print_success("Engine created successfully")

		# Тест генерации
		if hasattr(engine, 'generate'):
			print_info("Testing generation (5 tokens)...")
			test_prompt = "Hello, how are you?"

			try:
				result = engine.generate(test_prompt, max_tokens=5)
				print_success(f"Generation successful: {result[:100]}...")
				return True
			except Exception as e:
				print_error(f"Generation failed: {e}")

				# Дополнительная диагностика
				if "CUDA" in str(e) or "cuda" in str(e):
					print_warning("CUDA error detected. Try CPU mode.")
				return False
		else:
			print_warning("Engine has no 'generate' method")
			print_info(f"Engine methods: {[m for m in dir(engine) if not m.startswith('_')]}")
			return False

	except ImportError as e:
		print_error(f"Cannot import text_data_bench: {e}")
		print_info("Make sure text-data-bench is installed: pip install -e .")
		return False
	except Exception as e:
		print_error(f"Unexpected error: {type(e).__name__}: {e}")
		import traceback
		traceback.print_exc()
		return False

def check_nvidia_gpu():
	"""Проверка NVIDIA GPU через nvidia-smi"""
	print_header("5. NVIDIA GPU INFO")

	import subprocess

	try:
		result = subprocess.run(['nvidia-smi', '--query-gpu=name,memory.total,driver_version', '--format=csv,noheader'],
							  capture_output=True, text=True, timeout=5)

		if result.returncode == 0:
			gpu_info = result.stdout.strip().split(',')
			if len(gpu_info) >= 1:
				print_success(f"GPU: {gpu_info[0].strip()}")
				if len(gpu_info) >= 2:
					print_info(f"Memory: {gpu_info[1].strip()}")
				if len(gpu_info) >= 3:
					print_info(f"Driver: {gpu_info[2].strip()}")

			# Дополнительная информация
			result = subprocess.run(['nvidia-smi', '--query-gpu=compute_cap', '--format=csv,noheader'],
								  capture_output=True, text=True, timeout=5)
			if result.returncode == 0 and result.stdout.strip():
				print_info(f"Compute Capability: {result.stdout.strip()}")
		else:
			print_warning("nvidia-smi not available or no NVIDIA GPU found")

	except FileNotFoundError:
		print_warning("nvidia-smi not found in PATH")
	except subprocess.TimeoutExpired:
		print_warning("nvidia-smi timeout")
	except Exception as e:
		print_warning(f"Error checking GPU: {e}")

def main():
	"""Основная функция"""
	print(f"{Colors.BOLD}{Colors.HEADER}")
	print("╔════════════════════════════════════════════════════════════╗")
	print("║     GPU & LLM Diagnostic Tool v1.0                         ║")
	print("║     Проверка CUDA, llama-cpp-python и тест модели         ║")
	print("╚════════════════════════════════════════════════════════════╝")
	print(f"{Colors.END}")

	# Парсинг аргументов командной строки
	model_path = "./models/Qwen2-500M-Instruct-Q8_0.gguf"
	if len(sys.argv) > 1:
		model_path = sys.argv[1]

	print_info(f"Model path: {model_path}")

	# Выполнение проверок
	check_python_env()
	cuda_available = check_cuda_runtime()
	llama_ok = check_llama_cpp()

	if cuda_available:
		check_nvidia_gpu()

	if llama_ok and Path(model_path).exists():
		test_model_inference(model_path)
	elif llama_ok:
		print_warning("Skipping model test - model not found")
		print_info("Place a .gguf model file in the models directory")

	# Итоговый отчет
	print_header("DIAGNOSTIC SUMMARY")

	issues = []
	if not cuda_available:
		issues.append("- CUDA Runtime not found (install CUDA Toolkit)")

	if llama_ok:
		try:
			from llama_cpp import Llama
			if not hasattr(Llama, 'cuda'):
				issues.append("- llama-cpp-python is CPU only (reinstall with: pip install llama-cpp-python --index-url https://abetlen.github.io/llama-cpp-python/whl/cu124)")
		except:
			pass

	if issues:
		print_warning("Issues found:")
		for issue in issues:
			print(f"  {issue}")
	else:
		print_success("All checks passed! GPU is ready for inference.")

	print(f"\n{Colors.CYAN}For GPU installation run:{Colors.END}")
	print(f"  pip uninstall llama-cpp-python -y")
	print(f"  pip install llama-cpp-python==0.2.90 --index-url https://abetlen.github.io/llama-cpp-python/whl/cu124")

	print()

if __name__ == "__main__":
	main()
