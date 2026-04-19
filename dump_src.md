# Dump: src

**Root:** `C:\Users\AITISPEC\miniconda3\envs\TextDataBench\py\src`

---

## 📄 text_data_bench\__init__.py

```py
"""Text Data Bench - Automated data-centric pipeline."""
__version__ = "1.0.0"
```

---

## 📄 text_data_bench\__main__.py

```py
# src/text_data_bench/__main__.py
from text_data_bench.cli import app

def main():
	try:
		app()
	except KeyboardInterrupt:
		import sys
		sys.exit(130)

if __name__ == "__main__":
	main()
```

---

## 📄 text_data_bench\cli.py

```py
# src/text_data_bench/cli.py
import os, subprocess, sys
from pathlib import Path
import typer
from rich.console import Console
from rich.prompt import Prompt
from text_data_bench.config.loader import load_config
from text_data_bench.core.pipeline import run

console = Console()
app = typer.Typer(help="Automated data-centric benchmark & cleaning pipeline", add_completion=False)

# Оставляем для справки, но не используем для фильтрации списка
SUPPORTED_EXT = {".csv", ".tsv", ".parquet", ".feather", ".xlsx", ".xls", ".jsonl", ".json", ".txt", ".md", ".xml", ".h5", ".hdf5", ".arrow", ".pkl", ".pickle"}

def print_header():
	console.print("[bold cyan]┌──────────────────────────────────────────────────────┐[/bold cyan]")
	console.print("[bold cyan]│           TextDataBench CLI v1.1.0                 │[/bold cyan]")
	console.print("[bold cyan]└──────────────────────────────────────────────────────┘[/bold cyan]")

@app.command(name="menu")
def menu():
	print_header()
	fixture_dir = Path.cwd() / "tests/fixtures"
	fixture_dir.mkdir(parents=True, exist_ok=True)

	while True:
		console.print("\n[1] Test (pytest)")
		console.print("[2] Bench (run pipeline)")
		console.print("[3] Force Reinstall")
		console.print("[0] Exit")

		try:
			choice = Prompt.ask("\nSelect option", choices=["0", "1", "2", "3"], default="0")

			if choice == "0":
				console.print("\n[yellow]👋 Exiting...[/yellow]")
				raise typer.Exit()

			elif choice == "1":
				console.print("[blue]🧪 Running tests...[/blue]")
				res = subprocess.run([sys.executable, "-m", "pytest", "-v", "-m", "not slow"])
				console.print(f"\n[{'green' if res.returncode == 0 else 'red'}]Tests completed with code {res.returncode}[/{'green' if res.returncode == 0 else 'red'}]")

			elif choice == "2":
				SUPPORTED_EXT.add(".hf")
				files = sorted([f for f in fixture_dir.iterdir()
								if (f.is_file() or f.is_dir()) and not f.name.startswith(('.', '__'))
								and f.suffix.lower() in SUPPORTED_EXT])

				if not files:
					console.print(f"[red]❌ No datasets found in {fixture_dir}[/red]")
					continue

				console.print("\n[bold cyan]📂 Available Datasets:[/bold cyan]")
				console.print(f"  [0] ALL ({len(files)} datasets)")
				for i, f in enumerate(files, 1):
					size_mb = f.stat().st_size / (1024 * 1024)
					console.print(f"  [{i}] {f.name} ({size_mb:.2f} MB)")

				valid_choices = ["0"] + [str(i) for i in range(1, len(files) + 1)]
				idx = Prompt.ask("Select dataset number", choices=valid_choices)

				out_dir = Path.cwd() / "output"
				out_dir.mkdir(exist_ok=True)

				if idx == "0":
					console.print(f"\n[cyan]⚙️  Processing ALL ({len(files)} datasets)...[/cyan]")
					for f in files:
						cfg = load_config("configs/pipeline.yaml")
						cfg.output.report_path = str(out_dir / f"{f.name}.md")
						console.print(f"\n[bold]➤ {f.name}[/bold]")
						run(str(f), str(out_dir / f.name), cfg)
					console.print("\n[green]✅ Batch processing completed.[/green]")
				else:
					input_file = files[int(idx) - 1]
					cfg = load_config("configs/pipeline.yaml")
					cfg.output.report_path = str(out_dir / f"{input_file.name}.md")
					console.print(f"\n[cyan]⚙️  Processing: {input_file.name} → output/{input_file.name}[/cyan]")
					run(str(input_file), str(out_dir / input_file.name), cfg)

			elif choice == "3":
				console.print("[blue]🔄 Force reinstalling...[/blue]")
				script = Path(__file__).resolve().parents[2] / "force-reinstall.ps1"
				if script.exists():
					res = subprocess.run(["powershell", "-ExecutionPolicy", "Bypass", "-File", str(script)])
					if res.returncode == 0:
						console.print("[green]✅ Reinstall successful. Restart CLI to apply changes.[/green]")
					else:
						console.print("[red]❌ Reinstall failed.[/red]")
				else:
					console.print("[red]❌ force-reinstall.ps1 not found in project root.[/red]")

		except KeyboardInterrupt:
			console.print("\n[yellow]⏹ Interrupted. Exiting...[/yellow]")
			raise typer.Exit()
		except typer.Exit:
			sys.exit(0)
		except Exception as e:
			console.print(f"\n[red]💥 CLI Error: {e}[/red]")

if __name__ == "__main__":
	app()
```

---

## 📄 text_data_bench\config\__init__.py

```py

```

---

## 📄 text_data_bench\config\base.py

```py
from pydantic import BaseModel, Field
from typing import Optional

class PipelineCfg(BaseModel):
	prefer_gpu: bool = True
	text_col: Optional[str] = None

class FilterCfg(BaseModel):
	min_length: int = 10
	max_length: int = 10000
	remove_empty: bool = True

class DedupCfg(BaseModel):
	exact: bool = True
	fuzzy: bool = True
	fuzzy_threshold: float = 0.85
	num_perm: int = 128

class BalanceCfg(BaseModel):
	strategy: str = "stratified"
	group_col: Optional[str] = None
	seed: int = 42

class OutputCfg(BaseModel):
	format: str = "parquet"
	report_path: str = "output/report.md"

class LoggingCfg(BaseModel):
	level: str = "INFO"
	json_format: bool = False

class PipelineConfig(BaseModel):
	pipeline: PipelineCfg
	filters: FilterCfg = Field(default_factory=FilterCfg)
	dedup: DedupCfg = Field(default_factory=DedupCfg)
	balance: BalanceCfg = Field(default_factory=BalanceCfg)
	output: OutputCfg = Field(default_factory=OutputCfg)
	logging: LoggingCfg = Field(default_factory=LoggingCfg)
```

---

## 📄 text_data_bench\config\loader.py

```py
import yaml
from pathlib import Path
from rich.console import Console
from .base import PipelineConfig

console = Console()

def load_config(path: str = "configs/pipeline.yaml") -> PipelineConfig:
	p = Path(path)
	if not p.exists():
		console.print(f"[red]⚠ Config not found at {p}. Using defaults.[/red]")
		return PipelineConfig(pipeline={"prefer_gpu": True, "text_col": None})
	with open(p, encoding="utf-8") as f:
		data = yaml.safe_load(f)
	return PipelineConfig(**data)
```

---

## 📄 text_data_bench\core\__init__.py

```py

```

---

## 📄 text_data_bench\core\device.py

```py
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
```

---

## 📄 text_data_bench\core\pipeline.py

```py
# src/text_data_bench/core/pipeline.py
from pathlib import Path
from rich.console import Console
from text_data_bench.config.base import PipelineConfig
from text_data_bench.engines.core import load_and_standardize
from text_data_bench.processors.benchmark import compute_metrics
from text_data_bench.processors.filters import apply_filters
from text_data_bench.processors.dedup import deduplicate
from text_data_bench.processors.balancer import balance
from text_data_bench.io.reporters import generate_report
import polars as pl  # <-- Явный импорт исправляет NameError

console = Console()

def run(input_path: str, output_path: str, cfg: PipelineConfig) -> dict:
	stats = {"steps_completed": 0, "input": input_path, "output": output_path}
	try:
		console.print("[bold cyan]🚀 Starting pipeline...[/bold cyan]")
		console.print("[1/5] Loading & Standardizing...")
		df = load_and_standardize(input_path)

		TARGET_COL = "_tdb_text"
		console.print(f"[blue]📝 Processing column: '{TARGET_COL}'[/blue]")

		before = compute_metrics(df, TARGET_COL)
		console.print("[2/5] Filtering...")
		df = apply_filters(df, TARGET_COL, cfg.filters.min_length, cfg.filters.max_length, cfg.filters.remove_empty)
		stats["steps_completed"] += 1

		console.print("[3/5] Deduplicating...")
		df = deduplicate(df, TARGET_COL, cfg.dedup.exact, cfg.dedup.fuzzy, cfg.dedup.fuzzy_threshold, cfg.dedup.num_perm)
		stats["steps_completed"] += 1

		console.print("[4/5] Balancing...")
		df = balance(df, cfg.balance.strategy, cfg.balance.group_col, cfg.balance.seed)
		stats["steps_completed"] += 1

		after = compute_metrics(df, TARGET_COL)
		console.print("[5/5] Saving & Reporting...")
		Path(output_path).parent.mkdir(parents=True, exist_ok=True)
		out_ext = Path(output_path).suffix.lower()

		if out_ext == ".parquet":
			df.write_parquet(output_path)
		elif out_ext in (".csv", ".tsv"):
			# CSV/TSV не поддерживают вложенные типы → безопасный каст всех колонок в строки
			sep = "\t" if out_ext == ".tsv" else ","
			df.select(pl.all().cast(pl.String)).write_csv(output_path, separator=sep)
		else:
			# Для .pkl, .json, .md и прочих расширений сохраняем в parquet как стандарт
			fallback = Path(output_path).with_suffix(".parquet")
			df.write_parquet(fallback)
			console.print(f"[yellow]⚠ Saved as {fallback.name} (safe format)[/yellow]")

		generate_report(before, after, stats, cfg.output.report_path)
		console.print("[bold green]✅ Pipeline finished successfully.[/bold green]")
		return {"success": True, "input": input_path, "output": output_path}
	except KeyboardInterrupt:
		console.print("\n[yellow]⏹ Interrupted by user.[/yellow]")
		return {"success": False, "error": "KeyboardInterrupt"}
	except Exception as e:
		console.print(f"\n[red]💥 Critical error: {e}[/red]")
		import traceback
		console.print(traceback.format_exc())
		return {"success": False, "error": str(e)}
```

---

## 📄 text_data_bench\core\registry.py

```py
from typing import Callable, Dict
from pathlib import Path
import polars as pl

PARSER_REGISTRY: Dict[str, Callable[[Path], pl.LazyFrame]] = {}

def register(name: str) -> Callable:
	"""Декоратор для регистрации загрузчиков в глобальный реестр"""
	def decorator(func: Callable) -> Callable:
		PARSER_REGISTRY[name] = func
		return func
	return decorator
```

---

## 📄 text_data_bench\engines\__init__.py

```py

```

---

## 📄 text_data_bench\engines\core.py

```py
# src/text_data_bench/engines/core.py
"""14 Explicit Format Engines + Deterministic Standardizer"""
import polars as pl
import json, pickle, xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Callable
from rich.console import Console

console = Console()

# --- Безопасная нормализация ---
def _safe_to_flat_text(series: pl.Series) -> pl.Series:
	"""Гарантированно превращает любой Polars-тип в плоский string"""
	if series.dtype in (pl.String, pl.Utf8): return series
	try:
		return pl.Series([
			" ".join(str(item).strip() for item in row if item is not None)
			if isinstance(row, (list, tuple, set)) else str(row)
			for row in series.to_list()
		], dtype=pl.String)
	except Exception:
		return pl.Series([""] * len(series), dtype=pl.String)

def _standardize(df: pl.DataFrame, source_fmt: str) -> pl.DataFrame:
	"""Приводит любой датасет к единому виду с колонкой _tdb_text"""
	# 1. Ищем колонку с текстом: приоритет по именам, затем по длине
	candidates = []
	priority_names = {"text", "content", "utterance", "prompt", "response", "body", "description", "comment"}
	for col in df.columns:
		if df[col].dtype not in (pl.String, pl.Utf8, pl.List, pl.Struct, pl.Object):
			continue
		sample = df[col].drop_nulls().head(100)
		if len(sample) == 0: continue
		flat = _safe_to_flat_text(sample)
		med_len = flat.str.len_chars().median()
		if med_len and med_len > 10:
			score = 1000 if col.lower() in priority_names else med_len
			candidates.append((col, score))

	if not candidates:
		raise ValueError(f"[{source_fmt}] No string/list columns found. Cannot extract _tdb_text.")

	best_col = max(candidates, key=lambda x: x[1])[0]
	console.print(f"[blue]🔍 Extracting '{best_col}' → _tdb_text[/blue]")

	df = df.with_columns(_safe_to_flat_text(df[best_col]).alias("_tdb_text"))
	return df

# --- 14 Движков ---
def _load_csv(p: Path) -> pl.DataFrame: return _standardize(pl.read_csv(p), "CSV")
def _load_tsv(p: Path) -> pl.DataFrame: return _standardize(pl.read_csv(p, separator="\t"), "TSV")
def _load_parquet(p: Path) -> pl.DataFrame: return _standardize(pl.read_parquet(p), "Parquet")
def _load_feather(p: Path) -> pl.DataFrame: return _standardize(pl.read_ipc(p), "Feather")
def _load_excel(p: Path) -> pl.DataFrame: return _standardize(pl.read_excel(p, engine="openpyxl"), "Excel")
def _load_jsonl(p: Path) -> pl.DataFrame: return _standardize(pl.read_ndjson(p), "JSONL")

def _load_json(p: Path) -> pl.DataFrame:
	raw = json.loads(p.read_text(encoding="utf-8"))
	if isinstance(raw, list): return _standardize(pl.DataFrame(raw), "JSON")
	if isinstance(raw, dict): return _standardize(pl.DataFrame([raw]), "JSON")
	raise ValueError("[JSON] Root must be list or dict.")

def _load_txt(p: Path) -> pl.DataFrame:
	lines = [l.strip() for l in p.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
	return _standardize(pl.DataFrame({"text": lines}), "TXT")

def _load_md(p: Path) -> pl.DataFrame:
	lines = [l.strip() for l in p.read_text(encoding="utf-8", errors="ignore").splitlines() if l.strip()]
	return _standardize(pl.DataFrame({"content": lines}), "Markdown")

def _load_xml(p: Path) -> pl.DataFrame:
	tree = ET.parse(p)
	records = [{"xml_tag": e.tag, "xml_text": (e.text or "").strip()} for e in tree.iter() if e.text and e.text.strip()]
	if not records: raise ValueError("[XML] No extractable text nodes found.")
	return _standardize(pl.DataFrame(records), "XML")

def _load_hdf5(p: Path) -> pl.DataFrame:
	try: import h5py
	except ImportError: raise ImportError("[HDF5] Install h5py: pip install h5py")
	with h5py.File(p, "r") as f:
		for name, ds in f.items():
			if hasattr(ds, "astype") and ds.dtype.kind in ("U", "S", "O"):
				return _standardize(pl.DataFrame({"text": ds[:].astype(str)}), "HDF5")
	raise ValueError("[HDF5] No string datasets found.")

def _load_arrow(p: Path) -> pl.DataFrame: return _standardize(pl.read_ipc(p), "Arrow")

def _load_pickle(p: Path) -> pl.DataFrame:
	with open(p, "rb") as f: obj = pickle.load(f)
	if isinstance(obj, list):
		if obj and isinstance(obj[0], dict):
			return _standardize(pl.DataFrame(obj), "Pickle")
		return _standardize(pl.DataFrame({"text": obj}), "Pickle")
	if isinstance(obj, dict):
		return _standardize(pl.DataFrame([obj]), "Pickle")
	raise ValueError("[Pickle] Must contain dict, list, or tuple.")

def _load_hf_dataset(p: Path) -> pl.DataFrame:
	try: from datasets import load_from_disk
	except ImportError: raise ImportError("[HF] Install datasets: pip install datasets")
	ds = load_from_disk(p)
	# HF Datasets -> Polars
	return _standardize(pl.from_pandas(ds.to_pandas()), "HF Dataset")

ENGINE_MAP: Dict[str, Callable[[Path], pl.DataFrame]] = {
	".csv": _load_csv, ".tsv": _load_tsv, ".parquet": _load_parquet, ".feather": _load_feather,
	".xlsx": _load_excel, ".xls": _load_excel, ".jsonl": _load_jsonl, ".json": _load_json,
	".txt": _load_txt, ".md": _load_md, ".xml": _load_xml, ".h5": _load_hdf5, ".hdf5": _load_hdf5,
	".arrow": _load_arrow, ".pkl": _load_pickle, ".pickle": _load_pickle, ".hf": _load_hf_dataset,
}

def load_and_standardize(file_path: str) -> pl.DataFrame:
	ext = Path(file_path).suffix.lower()
	if ext not in ENGINE_MAP:
		raise ValueError(f"❌ Unsupported format: {ext}. Registered: {', '.join(ENGINE_MAP.keys())}")

	console.print(f"[cyan]⚙️  Routing to engine: {ext}[/cyan]")
	try:
		return ENGINE_MAP[ext](Path(file_path))
	except Exception as e:
		console.print(f"[red]💥 [{ext[1:].upper()}] Engine failed: {e}[/red]")
		raise
```

---

## 📄 text_data_bench\io\__init__.py

```py

```

---

## 📄 text_data_bench\io\loaders.py

```py
from pathlib import Path
import polars as pl, yaml
from rich.console import Console
from text_data_bench.core.registry import PARSER_REGISTRY, register
from text_data_bench.utils.magic_detector import detect_format

console = Console()

@register("polars_parquet")
def _scan_parquet(p: Path, **_): return pl.scan_parquet(p)

@register("polars_csv")
def _scan_csv(p: Path, **kw): return pl.scan_csv(p, **kw)

@register("polars_tsv")
def _scan_tsv(p: Path, **kw): return pl.scan_csv(p, separator="\t", **kw)

@register("polars_jsonl")
def _scan_jsonl(p: Path, **_): return pl.scan_ndjson(p)

@register("polars_json")
def _scan_json(p: Path, **_): return pl.read_json(p).lazy()

@register("text_lines")
def _load_text(p: Path, **kw):
	raw = p.read_text(encoding="utf-8", errors="ignore")
	lines = [l.strip() for l in raw.splitlines() if l.strip()]
	return pl.DataFrame({"text": lines}).lazy()

def _load_parser_cfg(path: str = "configs/parsers.yaml") -> list[dict]:
	p = Path(path)
	if not p.exists(): return []
	return yaml.safe_load(p.read_text(encoding="utf-8")).get("parsers", [])

def auto_load(file_path: str) -> pl.DataFrame:
	ext, _ = detect_format(file_path)
	p = Path(file_path)

	rules = _load_parser_cfg()
	handler, kwargs = None, {}
	for rule in rules:
		if ext in rule["extensions"]:
			handler, kwargs = rule["handler"], rule["kwargs"]
			break
	handler = handler or "unknown"

	if handler not in PARSER_REGISTRY:
		console.print(f"[red]🚫 Unsupported format: {ext}. Add loader to configs/parsers.yaml[/red]")
		raise ValueError(f"Unsupported format: {ext}")

	console.print(f"[blue]🔍 Matched {ext} → {handler}[/blue]")

	try:
		lf = PARSER_REGISTRY[handler](p, **kwargs)
		schema = lf.collect_schema()
		text_cols = [c for c, dt in schema.items() if dt in (pl.String, pl.Utf8) or str(dt).startswith(("List", "Struct"))]
		if not text_cols:
			raise ValueError("No suitable text columns found after loading.")

		target_col = text_cols[0]
		target_dtype = schema[target_col]

		if target_dtype in (pl.String, pl.Utf8):
			lf = lf.filter(pl.col(target_col).is_not_null() & (pl.col(target_col).str.len_chars() > 2))
		else:
			lf = lf.filter(pl.col(target_col).is_not_null())

		return lf.collect()
	except Exception as e:
		console.print(f"[red]❌ Load failed: {e}[/red]")
		raise
```

---

## 📄 text_data_bench\io\reporters.py

```py
from pathlib import Path
from datetime import datetime
from rich.console import Console

console = Console()

def _fmt(val) -> str:
	if val is None: return "N/A"
	if isinstance(val, float):
		if abs(val) < 0.01: return f"{val:.4f}"
		if abs(val) < 100: return f"{val:.2f}"
		return f"{val:,.0f}"
	if isinstance(val, int): return f"{val:,}"
	return str(val)

def _fmt_delta(before, after) -> str:
	if before is None or after is None: return "—"
	if not isinstance(before, (int, float)): return "—"
	diff = after - before
	if abs(diff) < 0.01 and isinstance(before, float): return "≈"
	sign = "↑" if diff > 0 else "↓" if diff < 0 else "•"
	val = f"{abs(diff):.2f}" if isinstance(before, float) else f"{int(abs(diff)):,}"
	return f"{sign} {val}"

def generate_report(before: dict, after: dict, stats: dict, out_path: str):
	p = Path(out_path)
	p.parent.mkdir(parents=True, exist_ok=True)

	ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	inp = stats.get("input", "N/A")
	out = stats.get("output", "N/A")

	md = f"""# 📊 TextDataBench Report
**Generated:** {ts}
**Input:** `{inp}` → **Output:** `{out}`
**Steps Completed:** {stats.get('steps_completed', 0)}/4

---

## 📈 Pipeline Summary
| Metric                     |     Value |
|:---------------------------|----------:|
| Rows Processed             | {_fmt(after.get('rows', 0))} |
| Dedup Removed              | {_fmt(before.get('rows', 0) - after.get('rows', 0))} |
| Avg Length (chars)         | {_fmt(after.get('avg_chars', 0))} |
| Avg Tokens                 | {_fmt(after.get('avg_tokens', 0))} |
| TTR (Lexical Diversity)    | {_fmt(after.get('ttr', 0))} |
| Shannon Entropy            | {_fmt(after.get('shannon_entropy', 0))} |
| Empty Ratio                | {_fmt(after.get('empty_ratio', 0))} |

---

## 🔄 Metric Delta (Before → After)
| Metric               |     Before |      After |   Change |
|:---------------------|-----------:|-----------:|---------:|
"""

	metrics = [
		("rows", "Rows"),
		("avg_chars", "Avg Chars"),
		("max_chars", "Max Chars"),
		("avg_tokens", "Avg Tokens"),
		("ttr", "TTR"),
		("shannon_entropy", "Entropy"),
		("empty_ratio", "Empty %"),
	]

	for key, label in metrics:
		b, a = before.get(key), after.get(key)
		md += f"| {label:<20} | {_fmt(b):>12} | {_fmt(a):>12} | {_fmt_delta(b, a):>10} |\n"

	md += f"\n---\n*Generated by TextDataBench v1.1.0*\n"
	p.write_text(md, encoding="utf-8")
	console.print(f"[green]✓ Report saved: {p}[/green]")
```

---

## 📄 text_data_bench\llm\__init__.py

```py

```

---

## 📄 text_data_bench\llm\engine.py

```py
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
```

---

## 📄 text_data_bench\llm\parser.py

```py
import json, re
from rich.console import Console
from .engine import get_engine

console = Console()

def parse_structured(text: str, model_path: str, ctx: int, prefer_gpu: bool) -> list[dict]:
	llm = get_engine(model_path, ctx, prefer_gpu)
	prompt = (
		"Extract structured tabular data from the raw text below.\n"
		"RULES: 1. Return ONLY a valid JSON array. 2. No markdown/explanations. 3. If unclear, return []\n\n"
		f"Raw text:\n{text[:6000]}\n\nJSON:"
	)
	for _ in range(2):
		res = llm(prompt, max_tokens=1024, temperature=0.05, stop=["```", "\n\n"])
		out = res["choices"][0]["text"].strip()
		match = re.search(r"\[.*\]", out, re.DOTALL)
		if match:
			try:
				data = json.loads(match.group(0))
				if isinstance(data, list): return data
			except json.JSONDecodeError: pass
		prompt += "\nStrict JSON array only."
	console.print("[yellow]⚠ LLM fallback failed. Returning empty list.[/yellow]")
	return []
```

---

## 📄 text_data_bench\processors\__init__.py

```py

```

---

## 📄 text_data_bench\processors\balancer.py

```py
import polars as pl
def balance(df: pl.DataFrame, strategy: str, group_col: str | None, seed: int) -> pl.DataFrame:
	if strategy == "stratified" and group_col and group_col in df.columns:
		parts = [g.sample(fraction=1.0, shuffle=True, seed=seed) for _, g in df.group_by(group_col)]
		df = pl.concat(parts)
	else:
		df = df.sample(fraction=1.0, shuffle=True, seed=seed)
	return df
```

---

## 📄 text_data_bench\processors\benchmark.py

```py
# src/text_data_bench/processors/benchmark.py
import polars as pl, math
from collections import Counter

def compute_metrics(df: pl.DataFrame, text_col: str) -> dict:
	if df.is_empty(): return {"rows": 0}
	txt = df[text_col]
	lengths = txt.str.len_chars()
	tokens = txt.str.split(" ").list.len()

	all_t = [w.lower() for toks in txt.str.split(" ").to_list() if toks for w in toks if len(w)>2]
	ttr = len(set(all_t)) / len(all_t) if all_t else 0.0

	char_counts = Counter("".join(txt.to_list()))
	total = sum(char_counts.values())
	entropy = -sum((c/total)*math.log2(c/total) for c in char_counts.values() if c>0) if total else 0.0

	return {
		"rows": len(df), "avg_chars": round(lengths.mean(), 1),
		"max_chars": lengths.max(), "avg_tokens": round(tokens.mean(), 1),
		"ttr": round(ttr, 4), "shannon_entropy": round(entropy, 2),
		"empty_ratio": round((txt.is_null() | (txt == "")).mean(), 4)
	}
```

---

## 📄 text_data_bench\processors\dedup.py

```py
import polars as pl
from datasketch import MinHash, MinHashLSH
from rich.console import Console
console = Console()
def deduplicate(df: pl.DataFrame, text_col: str, exact: bool, fuzzy: bool, threshold: float, num_perm: int) -> pl.DataFrame:
	initial = len(df)
	if exact: df = df.unique(subset=[text_col], keep="first")
	if fuzzy and len(df) > 0:
		console.print("[blue]🔍 Running MinHash LSH dedup...[/blue]")
		lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
		hashes = {}
		for i, txt in enumerate(df[text_col]):
			mh = MinHash(num_perm=num_perm)
			for t in txt.lower().split(): mh.update(t.encode("utf8"))
			lsh.insert(i, mh); hashes[i] = mh
		seen, keep = set(), []
		for i in range(len(df)):
			if i in seen: continue
			seen.add(i)
			for d in lsh.query(hashes[i]):
				if d != i: seen.add(d)
			keep.append(i)
		df = df[keep]
	console.print(f"[green]✓ Dedup complete: {initial} → {len(df)} rows[/green]")
	return df
```

---

## 📄 text_data_bench\processors\filters.py

```py
import polars as pl
def apply_filters(df: pl.DataFrame, text_col: str, min_len: int, max_len: int, remove_empty: bool) -> pl.DataFrame:
	mask = pl.col(text_col).is_not_null()
	if remove_empty: mask &= (pl.col(text_col).str.len_chars() > 0)
	if min_len: mask &= (pl.col(text_col).str.len_chars() >= min_len)
	if max_len: mask &= (pl.col(text_col).str.len_chars() <= max_len)
	return df.filter(mask)
```

---

## 📄 text_data_bench\tests\__init__.py

```py

```

---

## 📄 text_data_bench\utils\__init__.py

```py

```

---

## 📄 text_data_bench\utils\logger.py

```py

```

---

## 📄 text_data_bench\utils\magic_detector.py

```py
# src/text_data_bench/utils/magic_detector.py
from pathlib import Path

MAGIC_SIGNATURES = {
	b'PK\x03\x04': ('.zip', False),
	b'PAR1': ('.parquet', False),
	b'\x89HDF': ('.h5', False),
	b'\x93NUMPY': ('.npy', False),
	b'PICKLE': ('.pkl', False),
	b'ARROW1': ('.arrow', False),
	b'\xff\xd8\xff': ('.jpg', False),
	b'{': ('.json', True),
	b'<?xml': ('.xml', True),
}

def detect_format(filepath: str) -> tuple[str, bool]:
	p = Path(filepath)
	ext = p.suffix.lower()
	try:
		with open(p, 'rb') as f:
			header = f.read(16)
	except Exception:
		return ext, True

	for sig, (det_ext, is_text) in MAGIC_SIGNATURES.items():
		if header.startswith(sig):
			return det_ext, is_text
	return ext, True
```

---

## 📄 text_data_bench.egg-info\PKG-INFO

```text
Metadata-Version: 2.4
Name: text-data-bench
Version: 1.2.0
Summary: Data-centric pipeline with 14 explicit format engines
Requires-Python: >=3.12
Requires-Dist: polars>=1.39.3
Requires-Dist: pydantic>=2.13.2
Requires-Dist: typer>=0.24.1
Requires-Dist: rich>=15.0.0
Requires-Dist: datasketch>=1.10.0
Requires-Dist: plotly>=6.7.0
Requires-Dist: pyyaml>=6.0.3
Requires-Dist: openpyxl>=3.1.5
Requires-Dist: h5py>=3.16.0
Requires-Dist: datasets>=4.8.4
Requires-Dist: xlsxwriter>=3.2.9
```

---

## 📄 text_data_bench.egg-info\SOURCES.txt

```txt
README.md
pyproject.toml
src/text_data_bench/__init__.py
src/text_data_bench/__main__.py
src/text_data_bench/cli.py
src/text_data_bench.egg-info/PKG-INFO
src/text_data_bench.egg-info/SOURCES.txt
src/text_data_bench.egg-info/dependency_links.txt
src/text_data_bench.egg-info/entry_points.txt
src/text_data_bench.egg-info/requires.txt
src/text_data_bench.egg-info/top_level.txt
src/text_data_bench/config/__init__.py
src/text_data_bench/config/base.py
src/text_data_bench/config/loader.py
src/text_data_bench/core/__init__.py
src/text_data_bench/core/device.py
src/text_data_bench/core/pipeline.py
src/text_data_bench/core/registry.py
src/text_data_bench/engines/__init__.py
src/text_data_bench/engines/core.py
src/text_data_bench/io/__init__.py
src/text_data_bench/io/loaders.py
src/text_data_bench/io/reporters.py
src/text_data_bench/llm/__init__.py
src/text_data_bench/llm/engine.py
src/text_data_bench/llm/parser.py
src/text_data_bench/processors/__init__.py
src/text_data_bench/processors/balancer.py
src/text_data_bench/processors/benchmark.py
src/text_data_bench/processors/dedup.py
src/text_data_bench/processors/filters.py
src/text_data_bench/tests/__init__.py
src/text_data_bench/utils/__init__.py
src/text_data_bench/utils/logger.py
src/text_data_bench/utils/magic_detector.py
tests/test_pipeline.py
```

---

## 📄 text_data_bench.egg-info\dependency_links.txt

```txt

```

---

## 📄 text_data_bench.egg-info\entry_points.txt

```txt
[console_scripts]
databench = text_data_bench.cli:app
```

---

## 📄 text_data_bench.egg-info\requires.txt

```txt
polars>=1.39.3
pydantic>=2.13.2
typer>=0.24.1
rich>=15.0.0
datasketch>=1.10.0
plotly>=6.7.0
pyyaml>=6.0.3
openpyxl>=3.1.5
h5py>=3.16.0
datasets>=4.8.4
xlsxwriter>=3.2.9
```

---

## 📄 text_data_bench.egg-info\top_level.txt

```txt
text_data_bench
```

---

