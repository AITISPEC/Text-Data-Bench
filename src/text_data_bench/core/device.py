import subprocess
import logging
from pathlib import Path
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()

def detect_gpu() -> bool:
	"""Detect NVIDIA GPU availability."""
	try:
		subprocess.check_output(["nvidia-smi"], stderr=subprocess.DEVNULL)
		logger.info("NVIDIA GPU detected via nvidia-smi")
		return True
	except Exception:
		logger.debug("No NVIDIA GPU detected")
		return False

def detect_mps() -> bool:
	"""Detect Apple Silicon MPS availability."""
	try:
		import torch
		if hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
			logger.info("Apple MPS detected")
			return True
	except ImportError:
		pass
	return False

def get_llm_gpu_layers(prefer_gpu: bool) -> int:
	"""Determine number of GPU layers for LLM inference."""
	if prefer_gpu and detect_gpu():
		console.print("[green]✓ GPU detected. Routing LLM to CUDA.[/green]")
		return -1  # All layers on GPU
	if prefer_gpu and detect_mps():
		console.print("[green]✓ MPS detected. Routing LLM to Metal.[/green]")
		return -1
	console.print("[yellow]⚠ GPU unavailable. Falling back to CPU.[/yellow]")
	return 0

def validate_model_path(model_path: str | None) -> bool:
	"""Validate that GGUF model file exists."""
	if not model_path:
		return False
	path = Path(model_path)
	if not path.exists():
		logger.warning(f"Model file not found: {path}")
		return False
	if path.suffix != '.gguf':
		logger.warning(f"Expected .gguf file, got: {path.suffix}")
		return False
	return True
