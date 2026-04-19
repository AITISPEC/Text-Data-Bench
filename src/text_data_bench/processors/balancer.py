import polars as pl
def balance(df: pl.DataFrame, strategy: str, group_col: str | None, seed: int) -> pl.DataFrame:
	if strategy == "stratified" and group_col and group_col in df.columns:
		parts = [g.sample(fraction=1.0, shuffle=True, seed=seed) for _, g in df.group_by(group_col)]
		df = pl.concat(parts)
	else:
		df = df.sample(fraction=1.0, shuffle=True, seed=seed)
	return df
