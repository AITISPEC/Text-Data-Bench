# src/text_data_bench/engines/base.py
"""Базовые утилиты для стандартизации данных"""
import polars as pl
from rich.console import Console

console = Console()


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
