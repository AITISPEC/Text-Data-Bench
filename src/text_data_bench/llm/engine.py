from llama_cpp import Llama
from pathlib import Path
from rich.console import Console
from text_data_bench.core.device import get_llm_gpu_layers

console = Console()
_model_cache: Llama | None = None

def get_engine(model_path: str, ctx: int, prefer_gpu: bool) -> Llama:
	global _model_cache
	if _model_cache is not None:
		return _model_cache

	gpu_layers = get_llm_gpu_layers(prefer_gpu)
	console.print(f"[blue]📦 Loading GGUF: {Path(model_path).name} (ctx={ctx})[/blue]")

	try:
		_model_cache = Llama(
			model_path=model_path, n_ctx=ctx, n_gpu_layers=gpu_layers, verbose=False, mlock=False
		)
	except Exception as e:
		console.print(f"[red]⚠ GPU load failed: {e}. Retrying on CPU...[/red]")
		_model_cache = Llama(
			model_path=model_path, n_ctx=ctx, n_gpu_layers=0, verbose=False, mlock=False
		)
	return _model_cache
