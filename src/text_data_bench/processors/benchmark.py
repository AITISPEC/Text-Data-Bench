# src/text_data_bench/processors/benchmark.py
import polars as pl
import math
from collections import Counter

def compute_metrics(df: pl.DataFrame, text_col: str) -> dict:
	if df.is_empty():
		return {"rows": 0}
	txt = df[text_col]
	lengths = txt.str.len_chars()
	tokens = txt.str.split(" ").list.len()

	all_t = [w.lower() for toks in txt.str.split(" ").to_list() if toks for w in toks if len(w)>2]
	ttr = len(set(all_t)) / len(all_t) if all_t else 0.0

	char_counts = Counter("".join(txt.to_list()))
	total = sum(char_counts.values())
	entropy = -sum((c/total)*math.log2(c/total) for c in char_counts.values() if c>0) if total else 0.0

	return {
		"rows": len(df), "avg_chars": round(lengths.mean(), 1),
		"max_chars": lengths.max(), "avg_tokens": round(tokens.mean(), 1),
		"ttr": round(ttr, 4), "shannon_entropy": round(entropy, 2),
		"empty_ratio": round((txt.is_null() | (txt == "")).mean(), 4)
	}
