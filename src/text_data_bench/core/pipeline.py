# src/text_data_bench/core/pipeline.py
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
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
		
		with Progress(
			SpinnerColumn(),
			TextColumn("[progress.description]{task.description}"),
			BarColumn(),
			TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
			TimeRemainingColumn(),
			console=console
		) as progress:
			
			# Step 1: Loading
			load_task = progress.add_task("[cyan]Loading & Standardizing...", total=100)
			console.print("[1/5] Loading & Standardizing...")
			df = load_and_standardize(input_path)
			progress.update(load_task, completed=100)

			TARGET_COL = "_tdb_text"
			console.print(f"[blue]📝 Processing column: '{TARGET_COL}'[/blue]")

			before = compute_metrics(df, TARGET_COL)
			
			# Step 2: Filtering
			filter_task = progress.add_task("[cyan]Filtering...", total=100)
			console.print("[2/5] Filtering...")
			df = apply_filters(df, TARGET_COL, cfg.filters.min_length, cfg.filters.max_length, cfg.filters.remove_empty)
			stats["steps_completed"] += 1
			progress.update(filter_task, completed=100)

			# Step 3: Deduplicating
			dedup_task = progress.add_task("[cyan]Deduplicating...", total=100)
			console.print("[3/5] Deduplicating...")
			df = deduplicate(df, TARGET_COL, cfg.dedup.exact, cfg.dedup.fuzzy, cfg.dedup.fuzzy_threshold, cfg.dedup.num_perm)
			stats["steps_completed"] += 1
			progress.update(dedup_task, completed=100)

			# Step 4: Balancing
			balance_task = progress.add_task("[cyan]Balancing...", total=100)
			console.print("[4/5] Balancing...")
			df = balance(df, cfg.balance.strategy, cfg.balance.group_col, cfg.balance.seed)
			stats["steps_completed"] += 1
			progress.update(balance_task, completed=100)

			after = compute_metrics(df, TARGET_COL)
			
			# Step 5: Saving
			save_task = progress.add_task("[cyan]Saving & Reporting...", total=100)
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
			progress.update(save_task, completed=100)
		
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
