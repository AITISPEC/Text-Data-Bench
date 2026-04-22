# src/text_data_bench/engines/textual.py
"""Движки для текстовых форматов: TXT, Markdown, XML"""
import polars as pl
from pathlib import Path
import xml.etree.ElementTree as ET
from .base import _standardize


def load_txt(p: Path) -> pl.DataFrame:
    """Загрузка обычных текстовых файлов (по строкам)"""
    lines = [line.strip() for line in p.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
    if not lines:
        lines = [""]
    return _standardize(pl.DataFrame({"text": lines}), "TXT")


def load_md(p: Path) -> pl.DataFrame:
    """Загрузка Markdown файлов (по строкам)"""
    lines = [line.strip() for line in p.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
    if not lines:
        lines = [""]
    return _standardize(pl.DataFrame({"content": lines}), "Markdown")


def load_xml(p: Path) -> pl.DataFrame:
    """Загрузка XML файлов (извлечение текста из узлов)"""
    tree = ET.parse(p)
    records = [{"xml_tag": e.tag, "xml_text": (e.text or "").strip()} for e in tree.iter() if e.text and e.text.strip()]
    if not records:
        raise ValueError("[XML] No extractable text nodes found.")
    return _standardize(pl.DataFrame(records), "XML")
