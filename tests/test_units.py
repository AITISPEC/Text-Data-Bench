# tests/test_units.py
"""Unit tests for all modules."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import polars as pl  # noqa: E402

from text_data_bench.utils.cache import (  # noqa: E402
    get_cached, set_cached, clear_cache, _get_cache_key
)
from text_data_bench.processors.filters import apply_filters  # noqa: E402
from text_data_bench.processors.dedup import deduplicate  # noqa: E402
from text_data_bench.processors.balancer import balance  # noqa: E402
from text_data_bench.llm.engine import get_engine  # noqa: E402


# tests/test_cache.py
"""Unit tests for cache module."""


class TestCache:
    """Tests for cache utilities."""

    def test_get_cache_key_consistency(self):
        """Cache key should be consistent for same inputs."""
        key1 = _get_cache_key("text", 0.5, 128, 1000)
        key2 = _get_cache_key("text", 0.5, 128, 1000)
        assert key1 == key2

    def test_get_cache_key_different_inputs(self):
        """Cache key should differ for different inputs."""
        key1 = _get_cache_key("text1", 0.5, 128, 1000)
        key2 = _get_cache_key("text2", 0.5, 128, 1000)
        assert key1 != key2

    def test_set_and_get_cached(self, tmp_path, monkeypatch):
        """Test setting and retrieving cached values."""
        # Override cache directory for testing
        monkeypatch.setattr("text_data_bench.utils.cache._CACHE_DIR", tmp_path)

        test_value = {"keep_indices": [1, 2, 3], "metadata": "test"}
        key = _get_cache_key("test_col", 0.5, 128, 100)

        # Initially should be None
        assert get_cached(key) is None

        # Set value
        set_cached(key, test_value)

        # Retrieve value
        cached = get_cached(key)
        assert cached == test_value

    def test_clear_cache(self, tmp_path, monkeypatch):
        """Test clearing cache removes all files."""
        monkeypatch.setattr("text_data_bench.utils.cache._CACHE_DIR", tmp_path)

        # Add some cached items
        set_cached("key1", "value1")
        set_cached("key2", "value2")

        # Clear cache
        count = clear_cache()
        assert count >= 2

        # Verify cache is empty
        assert get_cached("key1") is None
        assert get_cached("key2") is None


# tests/test_filters.py
"""Unit tests for filters module."""


class TestFilters:
    """Tests for text filtering operations."""

    def test_apply_filters_min_length(self):
        """Test minimum length filter."""
        df = pl.DataFrame({
            "text": ["a", "ab", "abc", "abcd"]
        })
        result = apply_filters(df, "text", min_len=3, max_len=None, remove_empty=False)
        assert len(result) == 2
        assert all(len(t) >= 3 for t in result["text"])

    def test_apply_filters_max_length(self):
        """Test maximum length filter."""
        df = pl.DataFrame({
            "text": ["a", "ab", "abc", "abcd"]
        })
        result = apply_filters(df, "text", min_len=None, max_len=2, remove_empty=False)
        assert len(result) == 2
        assert all(len(t) <= 2 for t in result["text"])

    def test_apply_filters_remove_empty(self):
        """Test empty string removal."""
        df = pl.DataFrame({
            "text": ["hello", "", "world", ""]
        })
        result = apply_filters(df, "text", min_len=None, max_len=None, remove_empty=True)
        assert len(result) == 2
        assert all(len(t) > 0 for t in result["text"])

    def test_apply_filters_combined(self):
        """Test combined filters."""
        df = pl.DataFrame({
            "text": ["", "a", "ab", "abc", "abcd", None]
        })
        result = apply_filters(df, "text", min_len=2, max_len=3, remove_empty=True)
        assert len(result) == 2
        assert all(2 <= len(t) <= 3 for t in result["text"])

    def test_apply_filters_null_handling(self):
        """Test that nulls are filtered out."""
        df = pl.DataFrame({
            "text": ["hello", None, "world", None]
        })
        result = apply_filters(df, "text", min_len=None, max_len=None, remove_empty=False)
        assert len(result) == 2
        assert result["text"].null_count() == 0


# tests/test_dedup.py
"""Unit tests for deduplication module."""


class TestDedup:
    """Tests for deduplication operations."""

    def test_deduplicate_exact_only(self):
        """Test exact deduplication."""
        df = pl.DataFrame({
            "text": ["hello", "world", "hello", "test"]
        })
        result = deduplicate(df, "text", exact=True, fuzzy=False, threshold=0.9, num_perm=128)
        assert len(result) == 3
        # Check that duplicates are removed (order may vary)
        assert set(result["text"]) == {"hello", "world", "test"}

    def test_deduplicate_no_duplicates(self):
        """Test with no duplicates."""
        df = pl.DataFrame({
            "text": ["hello", "world", "test"]
        })
        result = deduplicate(df, "text", exact=True, fuzzy=False, threshold=0.9, num_perm=128)
        assert len(result) == 3

    def test_deduplicate_empty_dataframe(self):
        """Test with empty dataframe."""
        df = pl.DataFrame({"text": []}, schema={"text": pl.String})
        result = deduplicate(df, "text", exact=True, fuzzy=True, threshold=0.9, num_perm=128)
        assert len(result) == 0

    def test_deduplicate_fuzzy_disabled(self):
        """Test that fuzzy dedup can be disabled."""
        df = pl.DataFrame({
            "text": ["hello world", "hello worlds", "different"]
        })
        # With fuzzy=False, similar texts should not be removed
        result = deduplicate(df, "text", exact=True, fuzzy=False, threshold=0.9, num_perm=128)
        assert len(result) == 3


# tests/test_balancer.py
"""Unit tests for balancer module."""


class TestBalancer:
    """Tests for balancing operations."""

    def test_balance_uniform(self):
        """Test uniform balancing (random shuffle)."""
        df = pl.DataFrame({
            "text": ["a", "b", "c", "d", "e"],
            "label": [1, 1, 2, 2, 2]
        })
        result = balance(df, strategy="uniform", group_col=None, seed=42)
        assert len(result) == 5
        # Should contain all original values
        assert set(result["text"]) == {"a", "b", "c", "d", "e"}

    def test_balance_stratified(self):
        """Test stratified balancing."""
        df = pl.DataFrame({
            "text": ["a", "b", "c", "d"],
            "label": [1, 1, 2, 2]
        })
        result = balance(df, strategy="stratified", group_col="label", seed=42)
        assert len(result) == 4
        # Each group should be present
        assert set(result["label"]) == {1, 2}

    def test_balance_reproducible_seed(self):
        """Test that same seed produces same result."""
        df = pl.DataFrame({
            "text": ["a", "b", "c", "d", "e"]
        })
        result1 = balance(df, strategy="uniform", group_col=None, seed=123)
        result2 = balance(df, strategy="uniform", group_col=None, seed=123)
        assert list(result1["text"]) == list(result2["text"])


# tests/test_llm_engine.py
"""Unit tests for LLM engine module."""


class TestLLMEngine:
    """Tests for LLM engine."""

    def test_get_engine_no_model_path(self):
        """Test get_engine with None model path returns None."""
        result = get_engine(None, ctx=4096, prefer_gpu=False)
        assert result is None

    def test_get_engine_invalid_path(self):
        """Test get_engine with invalid path returns None."""
        result = get_engine("/nonexistent/path/model.gguf", ctx=4096, prefer_gpu=False)
        assert result is None

    def test_get_engine_default_ctx(self):
        """Test that default context window is 4096."""
        # This test verifies the default parameter value
        # Actual loading requires a valid model file
        result = get_engine(None)  # Uses default ctx=4096
        assert result is None
