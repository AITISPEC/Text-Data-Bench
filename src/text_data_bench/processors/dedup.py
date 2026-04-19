import polars as pl
from datasketch import MinHash, MinHashLSH
from rich.console import Console
console = Console()
def deduplicate(df: pl.DataFrame, text_col: str, exact: bool, fuzzy: bool, threshold: float, num_perm: int) -> pl.DataFrame:
	initial = len(df)
	if exact: df = df.unique(subset=[text_col], keep="first")
	if fuzzy and len(df) > 0:
		console.print("[blue]🔍 Running MinHash LSH dedup...[/blue]")
		lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
		hashes = {}
		for i, txt in enumerate(df[text_col]):
			mh = MinHash(num_perm=num_perm)
			for t in txt.lower().split(): mh.update(t.encode("utf8"))
			lsh.insert(i, mh); hashes[i] = mh
		seen, keep = set(), []
		for i in range(len(df)):
			if i in seen: continue
			seen.add(i)
			for d in lsh.query(hashes[i]):
				if d != i: seen.add(d)
			keep.append(i)
		df = df[keep]
	console.print(f"[green]✓ Dedup complete: {initial} → {len(df)} rows[/green]")
	return df
