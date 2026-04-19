# Dump: tests

**Root:** `C:\Users\AITISPEC\miniconda3\envs\TextDataBench\py\tests`

---

## 📄 __init__.py

```py

```

---

## 📄 conftest.py

```py
import pytest
from pathlib import Path
@pytest.fixture
def test_data_dir():
	return Path(__file__).parent / "fixtures"
```

---

## 📄 generate_fixtures.py

```py
# tests/generate_fixtures.py
"""Генератор синтетических фикстур с именами, соответствующими форматам"""
import polars as pl
import json, pickle, shutil, xml.etree.ElementTree as ET
from pathlib import Path

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE_DIR.mkdir(exist_ok=True)

DATA = [
	{"text": "Hello world this is a test sentence one.", "label": "pos"},
	{"text": "Polars is fast and efficient.", "label": "pos"},
	{"text": "Data processing requires care.", "label": "neg"},
	{"text": "Short text.", "label": "pos"},
	{"text": "Another example with more words to test length filters properly.", "label": "neg"},
]
df = pl.DataFrame(DATA)

def save_csv(): (FIXTURE_DIR / "csv.csv").write_text(df.write_csv())
def save_tsv(): (FIXTURE_DIR / "tsv.tsv").write_text(df.write_csv(separator="\t"))
def save_parquet(): df.write_parquet(FIXTURE_DIR / "parquet.parquet")
def save_feather(): df.write_ipc(FIXTURE_DIR / "feather.feather")
def save_jsonl(): (FIXTURE_DIR / "jsonl.jsonl").write_text(df.write_ndjson())
def save_json(): (FIXTURE_DIR / "json.json").write_text(json.dumps(df.to_dicts(), indent=2))
def save_txt(): (FIXTURE_DIR / "txt.txt").write_text("\n".join(d["text"] for d in DATA))
def save_md(): (FIXTURE_DIR / "md.md").write_text("\n".join(d["text"] for d in DATA))
def save_arrow(): df.write_ipc(FIXTURE_DIR / "arrow.arrow")

def save_xml():
	root = ET.Element("dataset")
	for d in DATA:
		item = ET.SubElement(root, "record")
		item.text = d["text"]
	tree = ET.ElementTree(root)
	tree.write(FIXTURE_DIR / "xml.xml", encoding="utf-8", xml_declaration=True)

def save_excel():
	try:
		df.write_excel(FIXTURE_DIR / "excel.xlsx")
	except ImportError:
		print("⚠ openpyxl/xlsxwriter missing → skipping Excel fixture")

def save_pickle():
	with open(FIXTURE_DIR / "pickle.pkl", "wb") as f:
		pickle.dump(DATA, f)

def save_hdf5():
	try:
		import h5py
		with h5py.File(FIXTURE_DIR / "hdf5.h5", "w") as f:
			f.create_dataset("text", data=[d["text"] for d in DATA], dtype=h5py.special_dtype(vlen=str))
	except ImportError:
		print("⚠ h5py missing → skipping HDF5 fixture")

def save_hf_dataset():
	try:
		from datasets import Dataset
		ds = Dataset.from_pandas(df.to_pandas())
		# HF Datasets сохраняется как директория
		ds.save_to_disk(str(FIXTURE_DIR / "hf.hf"))
	except ImportError:
		print("⚠ datasets missing → skipping HF Dataset fixture")

if __name__ == "__main__":
	# Безопасная очистка от предыдущих фикстур
	for item in FIXTURE_DIR.iterdir():
		if item.name.startswith(('.', '__')): continue
		if item.is_dir(): shutil.rmtree(item)
		else: item.unlink()

	generators = [
		save_csv, save_tsv, save_parquet, save_feather, save_jsonl, save_json,
		save_txt, save_md, save_xml, save_excel, save_arrow, save_pickle, save_hdf5, save_hf_dataset
	]
	for gen in generators:
		gen()

	count = sum(1 for _ in FIXTURE_DIR.iterdir())
	print(f"✅ Generated {count} fixtures in {FIXTURE_DIR}")
```

---

## 📄 test_pipeline.py

```py
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
	base, ext = filename, Path(filename).suffix.lower()
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
```

---

- `[SKIP BIN] fixtures\arrow.arrow`
## 📄 fixtures\csv.csv

```csv
text,label
Hello world this is a test sentence one.,pos
Polars is fast and efficient.,pos
Data processing requires care.,neg
Short text.,pos
Another example with more words to test length filters properly.,neg
```

---

- `[SKIP BIN] fixtures\excel.xlsx`
- `[SKIP BIN] fixtures\feather.feather`
- `[SKIP BIN] fixtures\hdf5.h5`
## 📄 fixtures\json.json

```json
[
  {
    "text": "Hello world this is a test sentence one.",
    "label": "pos"
  },
  {
    "text": "Polars is fast and efficient.",
    "label": "pos"
  },
  {
    "text": "Data processing requires care.",
    "label": "neg"
  },
  {
    "text": "Short text.",
    "label": "pos"
  },
  {
    "text": "Another example with more words to test length filters properly.",
    "label": "neg"
  }
]
```

---

## 📄 fixtures\jsonl.jsonl

```jsonl
{"text":"Hello world this is a test sentence one.","label":"pos"}
{"text":"Polars is fast and efficient.","label":"pos"}
{"text":"Data processing requires care.","label":"neg"}
{"text":"Short text.","label":"pos"}
{"text":"Another example with more words to test length filters properly.","label":"neg"}
```

---

## 📄 fixtures\md.md

```md
Hello world this is a test sentence one.
Polars is fast and efficient.
Data processing requires care.
Short text.
Another example with more words to test length filters properly.
```

---

- `[SKIP BIN] fixtures\parquet.parquet`
- `[SKIP BIN] fixtures\pickle.pkl`
## 📄 fixtures\tsv.tsv

```tsv
text	label
Hello world this is a test sentence one.	pos
Polars is fast and efficient.	pos
Data processing requires care.	neg
Short text.	pos
Another example with more words to test length filters properly.	neg
```

---

## 📄 fixtures\txt.txt

```txt
Hello world this is a test sentence one.
Polars is fast and efficient.
Data processing requires care.
Short text.
Another example with more words to test length filters properly.
```

---

## 📄 fixtures\xml.xml

```xml
<?xml version='1.0' encoding='utf-8'?>
<dataset><record>Hello world this is a test sentence one.</record><record>Polars is fast and efficient.</record><record>Data processing requires care.</record><record>Short text.</record><record>Another example with more words to test length filters properly.</record></dataset>
```

---

- `[SKIP BIN] fixtures\hf.hf\data-00000-of-00001.arrow`
## 📄 fixtures\hf.hf\dataset_info.json

```json
{
  "citation": "",
  "description": "",
  "features": {
    "text": {
      "dtype": "large_string",
      "_type": "Value"
    },
    "label": {
      "dtype": "large_string",
      "_type": "Value"
    }
  },
  "homepage": "",
  "license": ""
}
```

---

## 📄 fixtures\hf.hf\state.json

```json
{
  "_data_files": [
    {
      "filename": "data-00000-of-00001.arrow"
    }
  ],
  "_fingerprint": "dfb75333adb6d0e7",
  "_format_columns": null,
  "_format_kwargs": {},
  "_format_type": null,
  "_output_all_columns": false,
  "_split": null
}
```

---

