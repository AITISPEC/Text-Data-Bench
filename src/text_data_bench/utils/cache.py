# src/text_data_bench/utils/cache.py
"""LRU Cache for text processing operations."""
import hashlib
import json
from functools import lru_cache
from pathlib import Path
import pickle
from typing import Any, Optional

_CACHE_DIR = Path.home() / ".text_data_bench" / "cache"

def _get_cache_key(*args, **kwargs) -> str:
    """Generate a unique cache key from arguments."""
    key_data = json.dumps((args, sorted(kwargs.items())), default=str, sort_keys=True)
    return hashlib.md5(key_data.encode()).hexdigest()

def get_cached(key: str) -> Optional[Any]:
    """Retrieve value from disk cache if exists."""
    cache_file = _CACHE_DIR / f"{key}.pkl"
    if cache_file.exists():
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except Exception:
            return None
    return None

def set_cached(key: str, value: Any) -> None:
    """Store value in disk cache."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _CACHE_DIR / f"{key}.pkl"
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(value, f)
    except Exception:
        pass  # Silently fail if cache write fails

def clear_cache() -> int:
    """Clear all cached files. Returns number of files removed."""
    count = 0
    if _CACHE_DIR.exists():
        for f in _CACHE_DIR.glob("*.pkl"):
            try:
                f.unlink()
                count += 1
            except Exception:
                pass
    return count

@lru_cache(maxsize=1024)
def memoize_text_process(text: str, operation: str) -> str:
    """In-memory LRU cache for text processing results."""
    return text  # Placeholder - actual operation should be passed

__all__ = ['get_cached', 'set_cached', 'clear_cache', 'memoize_text_process', '_get_cache_key']
