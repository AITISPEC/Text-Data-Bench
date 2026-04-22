# src/text_data_bench/engines/tabular.py
"""Движки для табличных форматов: CSV, TSV, Parquet, Feather, Excel, JSONL, JSON"""
import polars as pl
from pathlib import Path
import json
from .base import _standardize


def load_csv(p: Path) -> pl.DataFrame:
    """Загрузка CSV файлов"""
    return _standardize(pl.read_csv(p), "CSV")


def load_tsv(p: Path) -> pl.DataFrame:
    """Загрузка TSV файлов"""
    return _standardize(pl.read_csv(p, separator="\t"), "TSV")


def load_parquet(p: Path) -> pl.DataFrame:
    """Загрузка Parquet файлов"""
    return _standardize(pl.read_parquet(p), "Parquet")


def load_feather(p: Path) -> pl.DataFrame:
    """Загрузка Feather файлов"""
    try:
        return _standardize(pl.read_ipc(p), "Feather")
    except pl.exceptions.ComputeError:
        try:
            import pyarrow.feather as pf
            table = pf.read_table(p)
            return _standardize(pl.from_arrow(table), "Feather")
        except Exception:
            raise


def load_excel(p: Path) -> pl.DataFrame:
    """Загрузка Excel файлов (.xlsx, .xls)"""
    return _standardize(pl.read_excel(p, engine="openpyxl"), "Excel")


def load_jsonl(p: Path) -> pl.DataFrame:
    """Загрузка JSONL (NDJSON) файлов"""
    return _standardize(pl.read_ndjson(p), "JSONL")


def load_json(p: Path) -> pl.DataFrame:
    """Загрузка JSON файлов (список или объект)"""
    raw = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return _standardize(pl.DataFrame(raw), "JSON")
    if isinstance(raw, dict):
        return _standardize(pl.DataFrame([raw]), "JSON")
    raise ValueError("[JSON] Root must be list or dict.")
