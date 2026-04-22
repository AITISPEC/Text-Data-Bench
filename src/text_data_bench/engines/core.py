# src/text_data_bench/engines/core.py
"""17+ Explicit Format Engines + Deterministic Standardizer

Модуль предоставляет единую точку входа для загрузки и стандартизации данных.
Движки разделены по категориям в отдельных файлах для удобства разработки и тестирования.
"""
from pathlib import Path
from typing import Dict, Callable
import polars as pl
from rich.console import Console

# Импорт базовых утилит
from .base import _safe_to_flat_text, _standardize

# Импорт движков по категориям
from .tabular import (
    load_csv, load_tsv, load_parquet, load_feather,
    load_excel, load_jsonl, load_json
)
from .textual import load_txt, load_md, load_xml
from .graph import load_gml, load_graphml, load_dot
from .specialized import load_hdf5, load_arrow, load_pickle, load_hf_dataset
from .numpy import load_npy, load_npz

console = Console()


# ==================== КАРТА РАСШИРЕНИЙ (17+ ФОРМАТОВ) ====================

ENGINE_MAP: Dict[str, Callable[[Path], pl.DataFrame]] = {
    # Табличные (7)
    ".csv": load_csv,
    ".tsv": load_tsv,
    ".parquet": load_parquet,
    ".feather": load_feather,
    ".xlsx": load_excel,
    ".xls": load_excel,
    ".jsonl": load_jsonl,
    ".json": load_json,
    # Текстовые (4)
    ".txt": load_txt,
    ".md": load_md,
    ".xml": load_xml,
    # Графовые (3)
    ".gml": load_gml,
    ".graphml": load_graphml,
    ".dot": load_dot,
    # Специализированные (5)
    ".h5": load_hdf5,
    ".hdf5": load_hdf5,
    ".arrow": load_arrow,
    ".pkl": load_pickle,
    ".pickle": load_pickle,
    ".hf": load_hf_dataset,
    # NumPy (2)
    ".npy": load_npy,
    ".npz": load_npz,
}


def load_and_standardize(file_path: str) -> pl.DataFrame:
    """Основная функция загрузки и стандартизации
    
    Args:
        file_path: Путь к файлу данных
        
    Returns:
        DataFrame с колонкой _tdb_text
        
    Raises:
        ValueError: Если формат не поддерживается или загрузка не удалась
    """
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
