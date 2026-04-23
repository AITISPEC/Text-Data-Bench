import polars as pl
import logging

logger = logging.getLogger(__name__)


def balance(df: pl.DataFrame, strategy: str, group_col: str | None, seed: int) -> pl.DataFrame:
	if strategy == "stratified" and group_col and group_col in df.columns:
		parts = [g.sample(fraction=1.0, shuffle=True, seed=seed) for _, g in df.group_by(group_col)]
		df = pl.concat(parts)
	elif strategy == "stratified":
		logger.warning(f"Stratified balancing requested but group_col={group_col} not found in columns. Falling back to uniform shuffle.")
		df = df.sample(fraction=1.0, shuffle=True, seed=seed)
	else:
		df = df.sample(fraction=1.0, shuffle=True, seed=seed)
	return df
