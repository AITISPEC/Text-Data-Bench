import yaml
from pathlib import Path
from rich.console import Console
from .base import PipelineConfig, PipelineCfg

console = Console()


def load_config(path: str = "configs/pipeline.yaml") -> PipelineConfig:
	p = Path(path)
	if not p.exists():
		console.print(f"[red]⚠ Config not found at {p}. Using defaults.[/red]")
		return PipelineConfig(pipeline=PipelineCfg(prefer_gpu=True, text_col=None))
	with open(p, encoding="utf-8") as f:
		data = yaml.safe_load(f)
	return PipelineConfig(**data)
