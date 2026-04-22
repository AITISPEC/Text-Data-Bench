# tests/test_pipeline.py
import pytest
from pathlib import Path
import importlib
from text_data_bench.config.loader import load_config
from text_data_bench.core.pipeline import run
import polars as pl

FIXTURES_DIR = Path(__file__).parent / "fixtures"

TEST_FILES = [
	f.name for f in FIXTURES_DIR.iterdir()
	if (f.is_file() or f.is_dir()) and not f.name.startswith(('.', '__'))
]


@pytest.mark.parametrize("filename", TEST_FILES)
def test_pipeline_runs(filename, tmp_path):
	sample = FIXTURES_DIR / filename
	cfg = load_config()
	cfg.filters.min_length = 3
	cfg.dedup.fuzzy = False
	cfg.balance.strategy = "uniform"

	out = tmp_path / f"out_{Path(filename).stem}.parquet"
	run(str(sample), str(out), cfg)

	assert out.exists(), f"Output file not created for {filename}"
	df = pl.read_parquet(out)
	assert len(df) > 0, f"Empty output for {filename}"
	assert "_tdb_text" in df.columns, f"Missing _tdb_text column in {filename}"
