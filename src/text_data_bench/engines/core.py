# src/text_data_bench/engines/core.py
"""17+ Explicit Format Engines + Deterministic Standardizer"""
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
	# 1. Если уже есть _tdb_text — ок
	if "_tdb_text" in df.columns:
		return df

	# 2. Ищем колонку с текстом: приоритет по именам, затем по длине
	candidates = []
	priority_names = {"text", "content", "utterance", "prompt", "response", "body", "description", "comment"}

	for col in df.columns:
		# Разрешаем строки, списки, структуры и числовые типы
		if df[col].dtype not in (pl.String, pl.Utf8, pl.List, pl.Struct, pl.Object,
								  pl.Float32, pl.Float64, pl.Int32, pl.Int64, pl.UInt32, pl.UInt64):
			continue
		sample = df[col].drop_nulls().head(100)
		if len(sample) == 0:
			continue
		flat = _safe_to_flat_text(sample)
		med_len = flat.str.len_chars().median()
		if med_len and med_len > 0:
			score = 1000 if col.lower() in priority_names else med_len
			candidates.append((col, score))

	if not candidates:
		raise ValueError(f"[{source_fmt}] No suitable columns found. Cannot extract _tdb_text.")

	best_col = max(candidates, key=lambda x: x[1])[0]
	console.print(f"[blue]🔍 Extracting '{best_col}' → _tdb_text[/blue]")

	df = df.with_columns(_safe_to_flat_text(df[best_col]).alias("_tdb_text"))
	return df


# ==================== 1. ТАБЛИЧНЫЕ ФОРМАТЫ ====================

def _load_csv(p: Path) -> pl.DataFrame:
	return _standardize(pl.read_csv(p), "CSV")


def _load_tsv(p: Path) -> pl.DataFrame:
	return _standardize(pl.read_csv(p, separator="\t"), "TSV")


def _load_parquet(p: Path) -> pl.DataFrame:
	return _standardize(pl.read_parquet(p), "Parquet")


def _load_feather(p: Path) -> pl.DataFrame:
	try:
		return _standardize(pl.read_ipc(p), "Feather")
	except pl.exceptions.ComputeError:
		try:
			import pyarrow.feather as pf
			table = pf.read_table(p)
			return _standardize(pl.from_arrow(table), "Feather")
		except Exception:
			raise


def _load_excel(p: Path) -> pl.DataFrame:
	return _standardize(pl.read_excel(p, engine="openpyxl"), "Excel")


def _load_jsonl(p: Path) -> pl.DataFrame:
	return _standardize(pl.read_ndjson(p), "JSONL")


def _load_json(p: Path) -> pl.DataFrame:
	raw = json.loads(p.read_text(encoding="utf-8"))
	if isinstance(raw, list):
		return _standardize(pl.DataFrame(raw), "JSON")
	if isinstance(raw, dict):
		return _standardize(pl.DataFrame([raw]), "JSON")
	raise ValueError("[JSON] Root must be list or dict.")


# ==================== 2. ТЕКСТОВЫЕ ФОРМАТЫ ====================

def _load_txt(p: Path) -> pl.DataFrame:
	lines = [line.strip() for line in p.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
	if not lines:
		lines = [""]
	return _standardize(pl.DataFrame({"text": lines}), "TXT")


def _load_md(p: Path) -> pl.DataFrame:
	lines = [line.strip() for line in p.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
	if not lines:
		lines = [""]
	return _standardize(pl.DataFrame({"content": lines}), "Markdown")


def _load_xml(p: Path) -> pl.DataFrame:
	tree = ET.parse(p)
	records = [{"xml_tag": e.tag, "xml_text": (e.text or "").strip()} for e in tree.iter() if e.text and e.text.strip()]
	if not records:
		raise ValueError("[XML] No extractable text nodes found.")
	return _standardize(pl.DataFrame(records), "XML")


# ==================== 3. ГРАФОВЫЕ ФОРМАТЫ ====================

def _load_gml(p: Path) -> pl.DataFrame:
	"""GML graph format -> edgelist"""
	try:
		import networkx as nx
	except ImportError:
		raise ImportError("[GML] Install networkx: pip install networkx")

	G = nx.read_gml(p)
	edges = list(G.edges())
	if edges:
		df = pl.DataFrame({"source": [str(e[0]) for e in edges], "target": [str(e[1]) for e in edges]})
	else:
		df = pl.DataFrame({"source": [], "target": []})
	return _standardize(df, "GML")


def _load_graphml(p: Path) -> pl.DataFrame:
	try:
		import networkx as nx
	except ImportError:
		raise ImportError("[GraphML] Install networkx: pip install networkx")

	G = nx.read_graphml(p)
	edges = list(G.edges())
	if edges:
		df = pl.DataFrame({"source": [str(e[0]) for e in edges], "target": [str(e[1]) for e in edges]})
	else:
		df = pl.DataFrame({"source": [], "target": []})
	return _standardize(df, "GraphML")


def _load_dot(p: Path) -> pl.DataFrame:
	try:
		import networkx as nx
	except ImportError:
		raise ImportError("[DOT] Install networkx: pip install networkx")

	# Пробуем pydot (легче устанавливается)
	try:
		G = nx.drawing.nx_pydot.read_dot(p)
	except ImportError:
		# Пробуем pygraphviz
		try:
			G = nx.nx_agraph.read_dot(p)
		except ImportError:
			raise ImportError("[DOT] Install pydot (pip install pydot) or pygraphviz")

	edges = list(G.edges())
	if edges:
		df = pl.DataFrame({"source": [str(e[0]) for e in edges], "target": [str(e[1]) for e in edges]})
	else:
		# Если нет рёбер, пробуем извлечь узлы
		nodes = list(G.nodes())
		if nodes:
			df = pl.DataFrame({"node": [str(n) for n in nodes]})
		else:
			df = pl.DataFrame({"source": [], "target": []})
	return _standardize(df, "DOT")


# ==================== 4. СПЕЦИАЛИЗИРОВАННЫЕ ФОРМАТЫ ====================

def _load_hdf5(p: Path) -> pl.DataFrame:
	try:
		import h5py
	except ImportError:
		raise ImportError("[HDF5] Install h5py: pip install h5py")

	with h5py.File(p, "r") as f:
		datasets = []
		def _collect_datasets(name, obj):
			if isinstance(obj, h5py.Dataset):
				datasets.append((name, obj))
		f.visititems(_collect_datasets)

		for name, ds in datasets:
			try:
				if ds.dtype.kind in ("U", "S", "O"):
					data = ds[:].astype(str)
					df = pl.DataFrame({"text": data})
					return _standardize(df, f"HDF5({name})")
				elif ds.dtype.kind in ("i", "f"):
					arr = ds[:]
					if arr.ndim == 1:
						df = pl.DataFrame({"value": arr})
					elif arr.ndim == 2:
						col_names = ds.attrs.get("column_names", [f"col_{i}" for i in range(arr.shape[1])])
						df = pl.DataFrame(arr.tolist(), schema=col_names, orient="row")
					else:
						df = pl.DataFrame({"value": arr.flatten()})
					return _standardize(df, f"HDF5({name})")
			except Exception:
				continue

	raise ValueError("[HDF5] No suitable datasets found.")


def _load_arrow(p: Path) -> pl.DataFrame:
	try:
		return _standardize(pl.read_ipc(p), "Arrow")
	except pl.exceptions.ComputeError:
		try:
			import pyarrow.ipc as ipc
			with ipc.open_file(p) as f:
				table = f.read_all()
			return _standardize(pl.from_arrow(table), "Arrow")
		except Exception:
			raise


def _load_pickle(p: Path) -> pl.DataFrame:
	with open(p, "rb") as f:
		obj = pickle.load(f)

	# pandas DataFrame
	if hasattr(obj, 'to_dict') and hasattr(obj, 'columns'):
		try:
			df = pl.DataFrame(obj)
			return _standardize(df, "Pickle(Pandas)")
		except Exception:
			try:
				df = pl.DataFrame(obj.to_dict())
				return _standardize(df, "Pickle(Pandas)")
			except Exception:
				pass

	# numpy array
	if hasattr(obj, 'shape') and hasattr(obj, 'tolist'):
		if obj.ndim == 1:
			df = pl.DataFrame({"value": obj.tolist()})
		elif obj.ndim == 2:
			columns = [f"col_{i}" for i in range(obj.shape[1])]
			df = pl.DataFrame(obj.tolist(), schema=columns, orient="row")
		else:
			df = pl.DataFrame({"value": obj.flatten().tolist()})
		return _standardize(df, "Pickle(Numpy)")

	# список словарей
	if isinstance(obj, list):
		if obj and isinstance(obj[0], dict):
			return _standardize(pl.DataFrame(obj), "Pickle")
		return _standardize(pl.DataFrame({"text": [str(x) for x in obj]}), "Pickle")

	# словарь
	if isinstance(obj, dict):
		return _standardize(pl.DataFrame([obj]), "Pickle")

	# tuple
	if isinstance(obj, tuple):
		return _standardize(pl.DataFrame({"text": [str(x) for x in obj]}), "Pickle")

	# объект с __dict__
	if hasattr(obj, '__dict__'):
		return _standardize(pl.DataFrame([obj.__dict__]), "Pickle")

	raise ValueError("[Pickle] Cannot convert object to DataFrame.")


def _load_hf_dataset(p: Path) -> pl.DataFrame:
	try:
		from datasets import load_from_disk
	except ImportError:
		raise ImportError("[HF] Install datasets: pip install datasets")
	ds = load_from_disk(p)
	return _standardize(pl.from_pandas(ds.to_pandas()), "HF Dataset")


# ==================== 5. NUMPY ФОРМАТЫ ====================

def _load_npy(p: Path) -> pl.DataFrame:
	import numpy as np
	data = np.load(p)
	if data.ndim == 1:
		df = pl.DataFrame({"value": data.tolist()})
	elif data.ndim == 2:
		columns = [f"col_{i}" for i in range(data.shape[1])]
		df = pl.DataFrame(data.tolist(), schema=columns, orient="row")
	else:
		df = pl.DataFrame({"value": data.flatten().tolist()})
	return _standardize(df, "NPY")


def _load_npz(p: Path) -> pl.DataFrame:
	import numpy as np
	data = np.load(p)
	first_key = list(data.keys())[0]
	arr = data[first_key]
	if arr.ndim == 1:
		df = pl.DataFrame({"value": arr.tolist()})
	elif arr.ndim == 2:
		columns = [f"col_{i}" for i in range(arr.shape[1])]
		df = pl.DataFrame(arr.tolist(), schema=columns, orient="row")
	else:
		df = pl.DataFrame({"value": arr.flatten().tolist()})
	return _standardize(df, f"NPZ({first_key})")


# ==================== КАРТА РАСШИРЕНИЙ (17+ ФОРМАТОВ) ====================

ENGINE_MAP: Dict[str, Callable[[Path], pl.DataFrame]] = {
	# Табличные (7)
	".csv": _load_csv,
	".tsv": _load_tsv,
	".parquet": _load_parquet,
	".feather": _load_feather,
	".xlsx": _load_excel,
	".xls": _load_excel,
	".jsonl": _load_jsonl,
	".json": _load_json,
	# Текстовые (4)
	".txt": _load_txt,
	".md": _load_md,
	".xml": _load_xml,
	# Графовые (3)
	".gml": _load_gml,
	".graphml": _load_graphml,
	".dot": _load_dot,
	# Специализированные (5)
	".h5": _load_hdf5,
	".hdf5": _load_hdf5,
	".arrow": _load_arrow,
	".pkl": _load_pickle,
	".pickle": _load_pickle,
	".hf": _load_hf_dataset,
	# NumPy (2)
	".npy": _load_npy,
	".npz": _load_npz,
}


def load_and_standardize(file_path: str) -> pl.DataFrame:
	"""Основная функция загрузки и стандартизации"""
	path = Path(file_path)
	ext = path.suffix.lower()

	# Обработка двойных расширений (.parquet.gzip)
	if ext == ".gzip" and path.stem.lower().endswith(".parquet"):
		ext = ".parquet"
		console.print(f"[cyan]⚠️ Detected .parquet.gzip → treating as Parquet[/cyan]")

	if ext not in ENGINE_MAP:
		raise ValueError(f"❌ Unsupported format: {ext}. Registered: {', '.join(ENGINE_MAP.keys())}")

	console.print(f"[cyan]⚙️  Routing to engine: {ext}[/cyan]")
	try:
		return ENGINE_MAP[ext](Path(file_path))
	except Exception as e:
		console.print(f"[red]💥 [{ext[1:].upper()}] Engine failed: {e}[/red]")
		raise
