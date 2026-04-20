import polars as pl
def apply_filters(df: pl.DataFrame, text_col: str, min_len: int, max_len: int, remove_empty: bool) -> pl.DataFrame:
	mask = pl.col(text_col).is_not_null()
	if remove_empty:
		mask &= (pl.col(text_col).str.len_chars() > 0)
	if min_len:
		mask &= (pl.col(text_col).str.len_chars() >= min_len)
	if max_len:
		mask &= (pl.col(text_col).str.len_chars() <= max_len)
	return df.filter(mask)
