import pytest
from libs.utils.dict import find_keys_by_pattern, update_nested_key


class TestDictUtils:
    """Unit tests for dictionary manipulation utilities."""

    def test_find_keys_by_pattern_simple(self):
        """
        GIVEN a dictionary with key 'password'
        THEN find_keys_by_pattern should yield its path and value
        WHEN searched with pattern 'pass'
        """
        data = {"user": "admin", "password": "123"}
        results = list(find_keys_by_pattern(data, "pass"))

        assert len(results) == 1
        assert results[0] == ("password", "123")

    def test_find_keys_by_pattern_nested(self):
        """
        GIVEN a nested structure with secret keys
        THEN return all matching paths including indices
        WHEN searched recursively
        """
        data = {
            "auth": {"token": "abc"},
            "items": [{"id": 1, "secret_id": "99"}, {"secret_id": "100"}],
        }
        # Match any key containing 'token' or 'secret'
        results = dict(find_keys_by_pattern(data, "token|secret"))

        assert results["auth.token"] == "abc"
        assert results["items[0].secret_id"] == "99"
        assert results["items[1].secret_id"] == "100"

    def test_find_keys_by_pattern_ignore_case(self):
        """
        GIVEN a dictionary with uppercase keys
        THEN regex should match regardless of casing
        WHEN ignore_case is set to True
        """
        data = {"API_KEY": "val"}
        results = list(find_keys_by_pattern(data, "api", ignore_case=True))

        assert len(results) == 1
        assert results[0][0] == "API_KEY"

    def test_update_nested_key_rename(self):
        """
        GIVEN a nested dictionary
        THEN the old key should be removed and new key added with same value
        WHEN update_nested_key is called without a new_value
        """
        data = {"a": {"b": {"c": 42}}}
        update_nested_key(data, "a.b.c", "d")

        assert "c" not in data["a"]["b"]
        assert data["a"]["b"]["d"] == 42

    def test_update_nested_key_value_override(self):
        """
        GIVEN a nested dictionary
        THEN the key should be renamed and its value updated
        WHEN update_nested_key is called with a new_value
        """
        data = {"meta": {"old": "data"}}
        update_nested_key(data, "meta.old", "new", new_value="refreshed")

        assert data["meta"]["new"] == "refreshed"
        assert "old" not in data["meta"]

    def test_update_nested_key_missing_path(self):
        """
        GIVEN an invalid path
        THEN raise a KeyError
        WHEN navigating the nested structure
        """
        data = {"a": 1}
        with pytest.raises(KeyError):
            update_nested_key(data, "b.c", "d")
