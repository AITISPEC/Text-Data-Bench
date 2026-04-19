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
