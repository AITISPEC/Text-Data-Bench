# src/text_data_bench/engines/base.py
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
	"""Приводит любой датасет к единому виду с колонкой _tdb_text.
	Объединяет ВСЕ существующие колонки в одну строку через пробел.
	"""
	if "_tdb_text" in df.columns:
		return df

	cols_to_use = [c for c in df.columns if c != "_tdb_text"]
	if not cols_to_use:
		raise ValueError(f"[{source_fmt}] No columns to process.")

	# Приводим каждую колонку к строковому типу
	string_cols = []
	for col in cols_to_use:
		dtype = df[col].dtype
		if dtype in (pl.String, pl.Utf8):
			string_cols.append(pl.col(col))
		elif dtype in (pl.List, pl.Struct, pl.Object):
			string_cols.append(pl.col(col).cast(pl.String))
		else:
			string_cols.append(pl.col(col).cast(pl.String))

	# Объединяем через пробел, null заменяем на пустую строку
	combined = pl.concat_str(string_cols, separator=" ").fill_null("")

	df = df.with_columns(combined.alias("_tdb_text"))
	console.print(f"[blue]🔍 Created _tdb_text from {len(cols_to_use)} columns[/blue]")
	return df
