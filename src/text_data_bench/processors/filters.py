# src/text_data_bench/processors/filters.py
import polars as pl
import logging

logger = logging.getLogger(__name__)


def apply_filters(df: pl.DataFrame, text_col: str,
				  min_len: int | None = None, max_len: int | None = None,
				  remove_empty: bool = True, adaptive_tolerance: float = 0.3) -> pl.DataFrame:
	"""
	Фильтрация строк на основе длины текста.
	Если min_len и max_len не заданы (оба None), используется адаптивный режим:
		- вычисляется мода длин строк в text_col
		- фильтруются строки, длина которых лежит в диапазоне
		  [mode * (1 - tolerance), mode * (1 + tolerance)]
	Иначе используется жёсткий диапазон [min_len, max_len].

	Parameters
	----------
	adaptive_tolerance : float, default=0.3
		Отклонение от моды в долях (30% = 0.3).
	"""
	initial = len(df)

	# Базовый фильтр: убираем null и пустые строки (если требуется)
	mask = pl.col(text_col).is_not_null()
	if remove_empty:
		mask &= (pl.col(text_col).str.len_chars() > 0)

	# Если заданы жёсткие границы — используем их
	if min_len is not None and max_len is not None:
		mask &= (pl.col(text_col).str.len_chars() >= min_len) & (pl.col(text_col).str.len_chars() <= max_len)
		df = df.filter(mask)
		logger.info(f"Filtered with hard limits [{min_len}, {max_len}]: {initial} → {len(df)} rows")
		return df

	# Адаптивный режим: вычисляем моду длины
	lengths = df.filter(mask).select(pl.col(text_col).str.len_chars()).to_series()
	if lengths.is_empty():
		logger.warning("No valid rows after null/empty removal. Returning empty DataFrame.")
		return df.filter(pl.lit(False))

	# Вычисляем моду (самое частое значение длины)
	mode_series = lengths.mode()
	if mode_series.is_empty():
		logger.warning("Could not determine mode length. Falling back to no length filter.")
		return df.filter(mask)

	mode_len = mode_series[0]
	low = mode_len * (1 - adaptive_tolerance)
	high = mode_len * (1 + adaptive_tolerance)

	mask &= (pl.col(text_col).str.len_chars() >= low) & (pl.col(text_col).str.len_chars() <= high)

	df = df.filter(mask)
	logger.info(f"Adaptive filter: mode={mode_len:.1f}, range=[{low:.1f}, {high:.1f}], {initial} → {len(df)} rows")
	return df
