# tests/test_pipeline.py
import pytest
from pathlib import Path
import importlib
from text_data_bench.config.loader import load_config
from text_data_bench.core.pipeline import run
import polars as pl

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Опциональные зависимости для пропуска тестов, если пакет не установлен
OPTIONAL_DEPS = {
	".xlsx": "openpyxl",
	".h5": "h5py",
	"sample_hf": "datasets",  # HF Dataset хранится как директория
}

def _should_skip(filename: str) -> str | None:
	ext = Path(filename).suffix.lower()
	for key, pkg in OPTIONAL_DEPS.items():
		if filename.startswith(key) or ext == key:
			try:
				importlib.import_module(pkg)
			except ImportError:
				return f"{pkg} not installed"
	return None

TEST_FILES = [
	f.name for f in FIXTURES_DIR.iterdir()
	if (f.is_file() or f.is_dir()) and not f.name.startswith(('.', '__'))
]

@pytest.mark.parametrize("filename", TEST_FILES)
def test_pipeline_runs(filename, tmp_path):
	skip_reason = _should_skip(filename)
	if skip_reason:
		pytest.skip(skip_reason)

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
