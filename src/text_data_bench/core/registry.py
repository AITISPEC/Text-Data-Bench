from typing import Callable, Dict
from pathlib import Path
import polars as pl

PARSER_REGISTRY: Dict[str, Callable[[Path], pl.LazyFrame]] = {}

def register(name: str) -> Callable:
	"""Декоратор для регистрации загрузчиков в глобальный реестр"""
	def decorator(func: Callable) -> Callable:
		PARSER_REGISTRY[name] = func
		return func
	return decorator
