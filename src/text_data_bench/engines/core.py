# src/text_data_bench/engines/core.py
"""17+ Explicit Format Engines + Deterministic Standardizer"""
from pathlib import Path
from typing import Dict, Callable
import polars as pl
import gzip
import bz2
import lzma
import tempfile
from rich.console import Console

from .base import _safe_to_flat_text, _standardize
from .tabular import (
	load_csv, load_tsv, load_parquet, load_feather,
	load_excel, load_jsonl, load_json
)
from .textual import load_txt, load_md, load_xml
from .graph import load_gml, load_graphml, load_dot
from .specialized import load_hdf5, load_arrow, load_pickle, load_hf_dataset
from .numpy import load_npy, load_npz

console = Console()

ENGINE_MAP: Dict[str, Callable[[Path], pl.DataFrame]] = {
	".csv": load_csv,
	".tsv": load_tsv,
	".parquet": load_parquet,
	".feather": load_feather,
	".xlsx": load_excel,
	".xls": load_excel,
	".jsonl": load_jsonl,
	".json": load_json,
	".txt": load_txt,
	".md": load_md,
	".xml": load_xml,
	".gml": load_gml,
	".graphml": load_graphml,
	".dot": load_dot,
	".h5": load_hdf5,
	".hdf5": load_hdf5,
	".arrow": load_arrow,
	".pkl": load_pickle,
	".pickle": load_pickle,
	".hf": load_hf_dataset,
	".npy": load_npy,
	".npz": load_npz,
}

# Поддержка сжатых форматов: расширение сжатия -> (открывающая функция, режим)
COMPRESSION_HANDLERS = {
	".gz": (gzip.open, "rt"),
	".bz2": (bz2.open, "rt"),
	".xz": (lzma.open, "rt"),
}


def _decompress_if_needed(path: Path) -> Path:
	"""Если файл имеет сжатое расширение, распаковывает во временный файл и возвращает путь к нему.
	Для несжатых возвращает исходный путь."""
	suffix = path.suffix.lower()
	if suffix in COMPRESSION_HANDLERS:
		open_func, mode = COMPRESSION_HANDLERS[suffix]
		# Создаём временный файл с тем же именем, но без сжатого расширения
		stem = path.stem
		# Определяем истинное расширение (например, .csv)
		true_ext = Path(stem).suffix.lower() if '.' in stem else ''
		if true_ext not in ENGINE_MAP:
			raise ValueError(f"Unsupported compressed format: {path} (inner extension {true_ext})")
		temp_fd, temp_path = tempfile.mkstemp(suffix=true_ext, prefix="tdb_")
		with os.fdopen(temp_fd, 'wb') as tmp_file:
			with open_func(path, mode) as compressed:
				tmp_file.write(compressed.read().encode('utf-8') if isinstance(compressed.read(), str) else compressed.read())
		console.print(f"[cyan]⚙️  Decompressed {suffix} → {temp_path}[/cyan]")
		return Path(temp_path)
	return path


def load_and_standardize(file_path: str) -> pl.DataFrame:
	path = Path(file_path)
	ext = path.suffix.lower()

	# Обработка двойных расширений (.parquet.gzip)
	if ext == ".gzip" and path.stem.lower().endswith(".parquet"):
		ext = ".parquet"
		console.print(f"[cyan]⚠️ Detected .parquet.gzip → treating as Parquet[/cyan]")

	# Распаковка сжатых файлов
	decompressed_path = _decompress_if_needed(path)
	if decompressed_path != path:
		# После распаковки используем новое расширение
		ext = decompressed_path.suffix.lower()
		path = decompressed_path

	if ext not in ENGINE_MAP:
		raise ValueError(f"❌ Unsupported format: {ext}. Registered: {', '.join(ENGINE_MAP.keys())}")

	console.print(f"[cyan]⚙️  Routing to engine: {ext}[/cyan]")
	try:
		return ENGINE_MAP[ext](path)
	except Exception as e:
		console.print(f"[red]💥 [{ext[1:].upper()}] Engine failed: {e}[/red]")
		raise
	finally:
		# Удаляем временный распакованный файл
		if decompressed_path != path and decompressed_path.exists():
			decompressed_path.unlink()
