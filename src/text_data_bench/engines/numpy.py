# src/text_data_bench/engines/numpy.py
"""Движки для NumPy форматов: NPY, NPZ"""
import polars as pl
from pathlib import Path
import numpy as np
from .base import _standardize


def load_npy(p: Path) -> pl.DataFrame:
    """Загрузка NPY файлов (numpy array)"""
    data = np.load(p)
    if data.ndim == 1:
        df = pl.DataFrame({"value": data.tolist()})
    elif data.ndim == 2:
        columns = [f"col_{i}" for i in range(data.shape[1])]
        df = pl.DataFrame(data.tolist(), schema=columns, orient="row")
    else:
        df = pl.DataFrame({"value": data.flatten().tolist()})
    return _standardize(df, "NPY")


def load_npz(p: Path) -> pl.DataFrame:
    """Загрузка NPZ файлов (архив numpy array)"""
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
