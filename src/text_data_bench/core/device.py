import subprocess
from rich.console import Console

console = Console()

def detect_gpu() -> bool:
	try:
		subprocess.check_output(["nvidia-smi"], stderr=subprocess.DEVNULL)
		return True
	except Exception:
		return False

def get_llm_gpu_layers(prefer_gpu: bool) -> int:
	if prefer_gpu and detect_gpu():
		console.print("[green]✓ GPU detected. Routing LLM to CUDA.[/green]")
		return -1
	console.print("[yellow]⚠ GPU unavailable. Falling back to CPU.[/yellow]")
	return 0
