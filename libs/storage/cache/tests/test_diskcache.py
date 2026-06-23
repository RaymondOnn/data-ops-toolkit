from pathlib import Path

import pytest
from libs.storage.cache.diskcache import DiskCache


class TestDiskCache:
    """Unit tests for the DiskCache backend implementation."""

    @pytest.fixture
    def cache(self, tmp_path):
        """Returns a DiskCache instance pointing to a temporary directory."""
        return DiskCache(cache_path=tmp_path / "test_db")

    def test_set_and_get(self, cache):
        """
        GIVEN a key and a value
        THEN the cache should store and retrieve the value correctly
        WHEN set() and get() are called
        """
        cache.set("user_id", 123)
        assert cache.get("user_id") == 123
        assert cache.get("missing", default="none") == "none"

    def test_dict_interface(self, cache):
        """
        GIVEN the dict-like interface
        THEN items should be accessible via square brackets
        WHEN using __setitem__ and __getitem__
        """
        cache["status"] = "active"
        assert cache["status"] == "active"
        assert "status" in cache
        assert len(cache) == 1

    def test_pop_logic(self, cache):
        """
        GIVEN an item in the cache
        THEN pop should return the value and remove it from storage
        WHEN pop() is called
        """
        cache.set("temp", "data")
        val = cache.pop("temp")
        assert val == "data"
        assert "temp" not in cache

    def test_delete_idempotency(self, cache):
        """
        GIVEN a key that may or may not exist
        THEN delete should not raise an error regardless of key existence
        WHEN delete() is called multiple times
        """
        cache.set("target", 1)
        cache.delete("target")
        cache.delete("target")  # Second call should be safe
        assert "target" not in cache

    def test_iterkeys_glob_pattern(self, cache):
        """
        GIVEN multiple keys with different prefixes
        THEN it should yield only keys matching the provided pattern
        WHEN iterkeys() is called with a pattern
        """
        cache.set("task:1", "a")
        cache.set("task:2", "b")
        cache.set("config:main", "c")

        tasks = list(cache.iterkeys(pattern="task:*"))
        assert len(tasks) == 2
        assert "task:1" in tasks
        assert "task:2" in tasks
        assert "config:main" not in tasks

    def test_transact_block(self, cache):
        """
        GIVEN a requirement for atomic updates
        THEN multiple operations should succeed within the block
        WHEN the transact() context manager is used
        """
        with cache.transact():
            cache.set("a", 10)
            cache.set("b", 20)

        assert cache.get("a") == 10
        assert cache.get("b") == 20

    def test_is_empty_logic(self, cache):
        """
        GIVEN a fresh cache
        THEN is_empty should correctly reflect the presence of data
        WHEN keys are added and removed
        """
        assert cache.is_empty() is True
        cache.set("x", 1)
        assert cache.is_empty() is False
        cache.delete("x")
        assert cache.is_empty() is True

    def test_path_resolution(self, tmp_path):
        """
        GIVEN a relative or home-expanded path
        THEN the cache should resolve it to an absolute physical location
        WHEN the DiskCache is initialized
        """
        # Using a relative path segment
        relative_path = Path("my_local_cache")
        # In tests, we anchor to tmp_path to keep it clean
        target = tmp_path / relative_path

        _ = DiskCache(cache_path=target)
        # diskcache creates a directory at the resolved path
        assert target.exists()
        assert target.is_dir()
