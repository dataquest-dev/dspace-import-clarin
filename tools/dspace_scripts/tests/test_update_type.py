"""
Unit tests for update_type helper functions.
No API calls are made – all tests operate on pure data.
"""
import os
import sys
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../update_type"))

for mod_name in ("dspace", "settings", "project_settings", "utils", "tqdm"):
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock.MagicMock()

sys.modules["project_settings"].settings = {
    "log_file": os.path.join(os.path.dirname(__file__), "__test.log"),
    "backend": {"endpoint": "http://localhost/api", "user": "test", "password": "test"},
}
sys.modules["settings"].env = {"dspace": {"handle_prefix": ["http://hdl.handle.net/"]}}
sys.modules["utils"].update_settings = lambda a, b: {
    "log_file": os.path.join(os.path.dirname(__file__), "__test.log"),
    "backend": {"endpoint": "http://localhost/api", "user": "test", "password": "test"},
    "dspace": {"handle_prefix": ["http://hdl.handle.net/"]},
    "update_type": {
        "mapping_file": os.path.join(os.path.dirname(__file__), "../update_type/mapping.json"),
    },
}
sys.modules["utils"].init_logging = lambda *a, **kw: None
sys.modules["utils"].load_env = lambda *a, **kw: None

from update_type import (  # noqa: E402
    map_type,
    build_patch,
    should_update,
    updater,
    DEFAULT_MAPPING,
)


# ---------------------------------------------------------------------------
# map_type
# ---------------------------------------------------------------------------

class TestMapType:

    def test_known_value_returns_mapped(self):
        assert map_type("J_ČLÁNEK", {"J_ČLÁNEK": "Article"}) == "Article"

    def test_unknown_value_returns_none(self):
        assert map_type("UNKNOWN", {"J_ČLÁNEK": "Article"}) is None

    def test_empty_string_returns_none(self):
        assert map_type("", {"J_ČLÁNEK": "Article"}) is None

    def test_empty_mapping_returns_none(self):
        assert map_type("J_ČLÁNEK", {}) is None

    def test_case_sensitive(self):
        assert map_type("j_článek", {"J_ČLÁNEK": "article"}) is None

    def test_multiple_entries(self):
        mapping = {"J_ČLÁNEK": "article", "BOOK": "Book"}
        assert map_type("BOOK", mapping) == "Book"
        assert map_type("J_ČLÁNEK", mapping) == "article"

    def test_default_mapping_contains_J_ČLÁNEK(self):
        assert map_type("J_ČLÁNEK", DEFAULT_MAPPING) == "article"


# ---------------------------------------------------------------------------
# build_patch
# ---------------------------------------------------------------------------

class TestBuildPatch:

    def test_structure(self):
        patch = build_patch(0, "Article")
        assert len(patch) == 1
        op = patch[0]
        assert op["op"] == "replace"
        assert op["path"] == "/metadata/dc.type/0/value"
        assert op["value"] == "Article"

    def test_index_in_path(self):
        patch = build_patch(2, "Article")
        assert patch[0]["path"] == "/metadata/dc.type/2/value"

    def test_value_propagated(self):
        patch = build_patch(1, "Book")
        assert patch[0]["value"] == "Book"

    def test_zero_index(self):
        patch = build_patch(0, "Article")
        assert patch[0]["path"] == "/metadata/dc.type/0/value"


# ---------------------------------------------------------------------------
# should_update
# ---------------------------------------------------------------------------

class TestShouldUpdate:

    def test_different_values_returns_true(self):
        assert should_update("J_ČLÁNEK", "Article") is True

    def test_same_value_returns_false(self):
        assert should_update("Article", "Article") is False

    def test_mapped_none_returns_false(self):
        assert should_update("J_ČLÁNEK", None) is False

    def test_empty_current_with_mapped_returns_true(self):
        assert should_update("", "Article") is True

    def test_mapped_none_even_if_current_empty(self):
        assert should_update("", None) is False


# ---------------------------------------------------------------------------
# updater
# ---------------------------------------------------------------------------

class TestUpdater:

    def _make_item(self, type_values: list) -> dict:
        return {
            "uuid": "item-uuid-1",
            "metadata": {
                "dc.type": [{"value": v} for v in type_values],
            },
        }

    def _make_dspace_be(self):
        be = mock.MagicMock()
        be.endpoint = "http://localhost/api"
        be.client.PatchOperation.REPLACE = "replace"
        be.client.api_patch.return_value = mock.MagicMock(ok=True)
        return be

    def test_known_type_gets_updated(self):
        item = self._make_item(["J_ČLÁNEK"])
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["updated"] == 1
        be.client.api_patch.assert_called_once()

    def test_unknown_type_not_patched(self):
        item = self._make_item(["UNKNOWN_TYPE"])
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["not_mapped"] == 1
        assert u.stats["updated"] == 0
        be.client.api_patch.assert_not_called()

    def test_already_correct_not_patched(self):
        item = self._make_item(["article"])
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["already_ok"] == 1
        assert u.stats["updated"] == 0
        be.client.api_patch.assert_not_called()

    def test_no_type_entry_counted(self):
        item = {"uuid": "item-uuid-2", "metadata": {}}
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["no_type"] == 1
        be.client.api_patch.assert_not_called()

    def test_dry_run_no_patch_called(self):
        item = self._make_item(["J_ČLÁNEK"])
        be = self._make_dspace_be()
        u = updater(be, dry_run=True)
        u.update_item(item)
        assert u.stats["updated"] == 1
        be.client.api_patch.assert_not_called()

    def test_patch_failure_counted(self):
        item = self._make_item(["J_ČLÁNEK"])
        be = self._make_dspace_be()
        be.client.api_patch.return_value = mock.MagicMock(ok=False)
        u = updater(be)
        u.update_item(item)
        assert u.stats["failed"] == 1
        assert u.stats["updated"] == 0

    def test_custom_mapping(self):
        item = self._make_item(["BOOK"])
        be = self._make_dspace_be()
        u = updater(be, mapping={"BOOK": "Book"})
        u.update_item(item)
        assert u.stats["updated"] == 1
        be.client.api_patch.assert_called_once()

    def test_multiple_type_entries_partial_match(self):
        item = self._make_item(["J_ČLÁNEK", "UNKNOWN"])
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["updated"] == 1
        assert u.stats["not_mapped"] == 1
        assert be.client.api_patch.call_count == 1

    def test_patch_api_called_with_correct_args(self):
        item = self._make_item(["J_ČLÁNEK"])
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        call_args = be.client.api_patch.call_args
        assert call_args[0][2] == "/metadata/dc.type/0/value"
        assert call_args[0][3] == "article"
