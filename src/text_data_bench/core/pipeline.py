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
