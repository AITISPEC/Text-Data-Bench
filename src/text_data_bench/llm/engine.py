try:
	from llama_cpp import Llama
	from pathlib import Path
	import logging
	from rich.console import Console
	from text_data_bench.core.device import get_llm_gpu_layers, validate_model_path

	logger = logging.getLogger(__name__)
	console = Console()
	_model_cache: Llama | None = None

	def get_engine(model_path: str | None, ctx: int = 512, prefer_gpu: bool = True) -> Llama | None:
		"""Load and cache GGUF model for LLM inference.
		
		Args:
			model_path: Path to GGUF model file. If None or invalid, returns None.
			ctx: Context window size (default: 512)
			prefer_gpu: Whether to prefer GPU acceleration (default: True)
		
		Returns:
			Llama instance or None if model cannot be loaded
		"""
		global _model_cache
		
		if _model_cache is not None:
			return _model_cache
		
		if not validate_model_path(model_path):
			console.print("[yellow]⚠ No valid GGUF model found. LLM features disabled.[/yellow]")
			logger.info("LLM engine disabled: no valid model path provided")
			return None
		
		gpu_layers = get_llm_gpu_layers(prefer_gpu)
		console.print(f"[blue]📦 Loading GGUF: {Path(model_path).name} (ctx={ctx})[/blue]")

		try:
			_model_cache = Llama(
				model_path=model_path, 
				n_ctx=ctx, 
				n_gpu_layers=gpu_layers, 
				verbose=False, 
				mlock=False,
				n_threads=None  # Auto-detect CPU threads
			)
			logger.info(f"Model loaded successfully with {gpu_layers} GPU layers")
			return _model_cache
		except Exception as e:
			logger.warning(f"GPU load failed: {e}. Retrying on CPU...")
			console.print(f"[yellow]⚠ GPU load failed: {e}. Retrying on CPU...[/yellow]")
			try:
				_model_cache = Llama(
					model_path=model_path, 
					n_ctx=ctx, 
					n_gpu_layers=0, 
					verbose=False, 
					mlock=False,
					n_threads=None
				)
				logger.info("Model loaded successfully on CPU")
				return _model_cache
			except Exception as e2:
				logger.error(f"Failed to load model on CPU: {e2}")
				console.print(f"[red]❌ Failed to load model: {e2}[/red]")
				return None
except ImportError:
	# llama-cpp-python not installed - provide stub functions
	import logging
	from rich.console import Console
	
	logger = logging.getLogger(__name__)
	console = Console()
	
	def get_engine(model_path: str | None, ctx: int = 512, prefer_gpu: bool = True):
		"""Stub function when llama-cpp-python is not installed."""
		console.print("[yellow]⚠ llama-cpp-python not installed. Install with: pip install llama-cpp-python[/yellow]")
		logger.warning("get_engine called but llama-cpp-python is not installed")
		return None
	
	Llama = None

__all__ = ['get_engine']
