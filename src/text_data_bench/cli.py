# src/text_data_bench/cli.py
import subprocess
import sys
from pathlib import Path
import typer
from rich.console import Console
from rich.prompt import Prompt
from text_data_bench.config.loader import load_config
from text_data_bench.core.pipeline import run

console = Console()
app = typer.Typer(help="Automated data-centric benchmark & cleaning pipeline", add_completion=False)

# Все поддерживаемые расширения (синхронизировано с ENGINE_MAP)
SUPPORTED_EXT = {
	".csv", ".tsv", ".parquet", ".feather", ".xlsx", ".xls",
	".jsonl", ".json", ".txt", ".md", ".xml", ".h5", ".hdf5",
	".arrow", ".pkl", ".pickle", ".hf", ".npy", ".npz",
	".gml", ".graphml", ".dot"
}


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
		console.print("[0] Exit")

		try:
			choice = Prompt.ask("\nSelect option", choices=["0", "1", "2"], default="0")

			if choice == "0":
				console.print("\n[yellow]👋 Exiting...[/yellow]")
				raise typer.Exit()

			elif choice == "1":
				console.print("[blue]🧪 Running tests...[/blue]")
				res = subprocess.run([sys.executable, "-m", "pytest", "-v", "-m", "not slow"])
				console.print(f"\n[{'green' if res.returncode == 0 else 'red'}]Tests completed with code {res.returncode}[/{'green' if res.returncode == 0 else 'red'}]")

			elif choice == "2":
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

		except KeyboardInterrupt:
			console.print("\n[yellow]⏹ Interrupted. Exiting...[/yellow]")
			raise typer.Exit()
		except typer.Exit:
			sys.exit(0)
		except Exception as e:
			console.print(f"\n[red]💥 CLI Error: {e}[/red]")


if __name__ == "__main__":
	app()
