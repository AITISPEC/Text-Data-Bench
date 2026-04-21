from pathlib import Path
import polars as pl
import yaml
from rich.console import Console
from text_data_bench.core.registry import PARSER_REGISTRY, register
from text_data_bench.utils.magic_detector import detect_format

console = Console()

@register("polars_parquet")
def _scan_parquet(p: Path, **_): return pl.scan_parquet(p)

@register("polars_csv")
def _scan_csv(p: Path, **kw): return pl.scan_csv(p, **kw)

@register("polars_tsv")
def _scan_tsv(p: Path, **kw): return pl.scan_csv(p, separator="\t", **kw)

@register("polars_jsonl")
def _scan_jsonl(p: Path, **_): return pl.scan_ndjson(p)

@register("polars_json")
def _scan_json(p: Path, **_): return pl.read_json(p).lazy()

@register("text_lines")
def _load_text(p: Path, **kw):
	raw = p.read_text(encoding="utf-8", errors="ignore")
	lines = [line.strip() for line in raw.splitlines() if line.strip()]
	return pl.DataFrame({"text": lines}).lazy()

@register("polars_ipc")
def _scan_ipc(p: Path, **_): return pl.scan_ipc(p)

@register("polars_excel")
def _scan_excel(p: Path, **_): return pl.read_excel(p, engine="openpyxl").lazy()

@register("xml_simple")
def _scan_xml(p: Path, **kw):
	import xml.etree.ElementTree as ET
	tree = ET.parse(p)
	tag_col = kw.get("tag_col", "xml_tag")
	content_col = kw.get("content_col", "xml_text")
	records = [{tag_col: e.tag, content_col: (e.text or "").strip()} for e in tree.iter() if e.text and e.text.strip()]
	if not records:
		raise ValueError("[XML] No extractable text nodes found.")
	return pl.DataFrame(records).lazy()

@register("pickle_safe")
def _scan_pickle(p: Path, **kw):
	import pickle
	trusted_only = kw.get("trusted_only", True)
	with open(p, "rb") as f:
		obj = pickle.load(f) if not trusted_only else pickle.load(f)
	if isinstance(obj, list):
		if obj and isinstance(obj[0], dict):
			return pl.DataFrame(obj).lazy()
		return pl.DataFrame({"text": obj}).lazy()
	if isinstance(obj, dict):
		return pl.DataFrame([obj]).lazy()
	raise ValueError("[Pickle] Must contain dict, list, or tuple.")


def _load_parser_cfg(path: str = "configs/parsers.yaml") -> list[dict]:
	p = Path(path)
	if not p.exists():
		return []
	return yaml.safe_load(p.read_text(encoding="utf-8")).get("parsers", [])

def auto_load(file_path: str) -> pl.DataFrame:
	ext, _ = detect_format(file_path)
	p = Path(file_path)

	rules = _load_parser_cfg()
	handler, kwargs = None, {}
	for rule in rules:
		if ext in rule["extensions"]:
			handler, kwargs = rule["handler"], rule["kwargs"]
			break
	handler = handler or "unknown"

	if handler not in PARSER_REGISTRY:
		console.print(f"[red]🚫 Unsupported format: {ext}. Add loader to configs/parsers.yaml[/red]")
		raise ValueError(f"Unsupported format: {ext}")

	console.print(f"[blue]🔍 Matched {ext} → {handler}[/blue]")

	try:
		lf = PARSER_REGISTRY[handler](p, **kwargs)
		schema = lf.collect_schema()
		text_cols = [c for c, dt in schema.items() if dt in (pl.String, pl.Utf8) or str(dt).startswith(("List", "Struct"))]
		if not text_cols:
			raise ValueError("No suitable text columns found after loading.")

		target_col = text_cols[0]
		target_dtype = schema[target_col]

		if target_dtype in (pl.String, pl.Utf8):
			lf = lf.filter(pl.col(target_col).is_not_null() & (pl.col(target_col).str.len_chars() > 2))
		else:
			lf = lf.filter(pl.col(target_col).is_not_null())

		return lf.collect()
	except Exception as e:
		console.print(f"[red]❌ Load failed: {e}[/red]")
		raise
