# src/text_data_bench/engines/core.py
"""14 Explicit Format Engines + Deterministic Standardizer"""
import polars as pl
import json
import pickle
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Callable
from rich.console import Console

console = Console()

# --- Безопасная нормализация ---
def _safe_to_flat_text(series: pl.Series):
	"""Гарантированно превращает любой Polars-тип в плоский string"""
	if series.dtype in (pl.String, pl.Utf8):
		return series
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
		if len(sample) == 0:
			continue
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
	if isinstance(raw, list):
		return _standardize(pl.DataFrame(raw), "JSON")
	if isinstance(raw, dict):
		return _standardize(pl.DataFrame([raw]), "JSON")
	raise ValueError("[JSON] Root must be list or dict.")

def _load_txt(p: Path) -> pl.DataFrame:
	lines = [line.strip() for line in p.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
	return _standardize(pl.DataFrame({"text": lines}), "TXT")

def _load_md(p: Path) -> pl.DataFrame:
	lines = [line.strip() for line in p.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
	return _standardize(pl.DataFrame({"content": lines}), "Markdown")

def _load_xml(p: Path) -> pl.DataFrame:
	tree = ET.parse(p)
	records = [{"xml_tag": e.tag, "xml_text": (e.text or "").strip()} for e in tree.iter() if e.text and e.text.strip()]
	if not records:
		raise ValueError("[XML] No extractable text nodes found.")
	return _standardize(pl.DataFrame(records), "XML")

def _load_hdf5(p: Path) -> pl.DataFrame:
	try:
		import h5py
	except ImportError:
		raise ImportError("[HDF5] Install h5py: pip install h5py")
	with h5py.File(p, "r") as f:
		for name, ds in f.items():
			if hasattr(ds, "astype") and ds.dtype.kind in ("U", "S", "O"):
				return _standardize(pl.DataFrame({"text": ds[:].astype(str)}), "HDF5")
	raise ValueError("[HDF5] No string datasets found.")

def _load_arrow(p: Path) -> pl.DataFrame: return _standardize(pl.read_ipc(p), "Arrow")

def _load_pickle(p: Path) -> pl.DataFrame:
	with open(p, "rb") as f:
		obj = pickle.load(f)
	if isinstance(obj, list):
		if obj and isinstance(obj[0], dict):
			return _standardize(pl.DataFrame(obj), "Pickle")
		return _standardize(pl.DataFrame({"text": obj}), "Pickle")
	if isinstance(obj, dict):
		return _standardize(pl.DataFrame([obj]), "Pickle")
	raise ValueError("[Pickle] Must contain dict, list, or tuple.")

def _load_hf_dataset(p: Path) -> pl.DataFrame:
	try:
		from datasets import load_from_disk
	except ImportError:
		raise ImportError("[HF] Install datasets: pip install datasets")
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
