"""
Unit tests for fix_identifier_uri helper functions.
No API calls are made – all tests operate on pure data.
"""
import sys
import os

# Make the fix_identifier_uri directory importable without requiring the full DSpace
# environment (src/ imports) to be present.  We import only the module
# functions that do *not* depend on any external libraries.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../fix_identifier_uri"))

# Patch heavy dependencies before they are imported so the module can be
# loaded in a plain Python environment (e.g. a CI runner without DSpace libs).
import unittest.mock as mock

for mod_name in ("dspace", "settings", "mendelu_settings", "utils", "tqdm", "requests"):
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock.MagicMock()

sys.modules["mendelu_settings"].settings = {
    "log_file": os.path.join(os.path.dirname(__file__), "__test.log"),
    "backend": {
        "endpoint": "http://localhost/api",
        "user": "test",
        "password": "test",
    },
}
sys.modules["settings"].env = {"dspace": {"handle_prefix": ["http://hdl.handle.net/", "https://hdl.handle.net/"]}}
sys.modules["utils"].update_settings = lambda a, b: {
    "log_file": os.path.join(os.path.dirname(__file__), "__test.log"),
    "backend": {"endpoint": "http://localhost/api", "user": "test", "password": "test"},
    "dspace": {"handle_prefix": ["http://hdl.handle.net/", "https://hdl.handle.net/"]},
}
sys.modules["utils"].init_logging = lambda *a, **kw: None

from tools.mendelu.fix_identifier_uri.fix_identifier_uri import (  # noqa: E402
    parse_handle,
    build_handle_url,
    needs_update,
    build_patch,
    fixer,
)


# ---------------------------------------------------------------------------
# parse_handle
# ---------------------------------------------------------------------------

class TestParseHandle:

    def test_typical_url(self):
        url = "https://dspace.example.org/handle/20.500.12698/1785"
        assert parse_handle(url) == "20.500.12698/1785"

    def test_simple_numeric_handle(self):
        url = "https://dspace.example.org/handle/123456789/1095"
        assert parse_handle(url) == "123456789/1095"

    def test_dotted_prefix_handle(self):
        url = "https://dspace.example.org/handle/20.500.12698/1370"
        assert parse_handle(url) == "20.500.12698/1370"

    def test_url_with_http(self):
        url = "http://dspace.example.org/handle/20.500.12698/42"
        assert parse_handle(url) == "20.500.12698/42"

    def test_url_with_query_string(self):
        url = "https://dspace.example.org/handle/20.500.12698/99?locale=en"
        assert parse_handle(url) == "20.500.12698/99"

    def test_url_with_fragment(self):
        url = "https://dspace.example.org/handle/20.500.12698/99#section"
        assert parse_handle(url) == "20.500.12698/99"

    def test_url_with_path_prefix(self):
        url = "https://dspace.example.org/xmlui/handle/20.500.12698/1785"
        assert parse_handle(url) == "20.500.12698/1785"

    def test_no_handle_in_url(self):
        url = "https://dspace.example.org/items/some-uuid"
        assert parse_handle(url) is None

    def test_already_hdl_http_url(self):
        # http hdl.handle.net URL – handle should be extracted directly
        url = "http://hdl.handle.net/20.500.12698/1785"
        assert parse_handle(url) == "20.500.12698/1785"

    def test_already_hdl_https_url(self):
        # https hdl.handle.net URL – handle should be extracted directly
        url = "https://hdl.handle.net/20.500.12698/1785"
        assert parse_handle(url) == "20.500.12698/1785"

    def test_empty_string(self):
        assert parse_handle("") is None


# ---------------------------------------------------------------------------
# build_handle_url
# ---------------------------------------------------------------------------

class TestBuildHandleUrl:

    def test_standard_handle(self):
        assert build_handle_url("20.500.12698/1785") == "http://hdl.handle.net/20.500.12698/1785"

    def test_short_handle(self):
        assert build_handle_url("123/456") == "http://hdl.handle.net/123/456"


# ---------------------------------------------------------------------------
# needs_update
# ---------------------------------------------------------------------------

class TestNeedsUpdate:

    def test_already_correct_http(self):
        assert needs_update("http://hdl.handle.net/20.500.12698/1785") is False

    def test_http_wrong_domain(self):
        assert needs_update("http://dspace.example.org/handle/20.500.12698/1785") is True

    def test_https_hdl(self):
        # https variant does NOT start with "http://hdl.handle.net/" so needs update
        assert needs_update("https://hdl.handle.net/20.500.12698/1785") is True

    def test_random_url(self):
        assert needs_update("https://example.org/handle/20.500.12698/1785") is True

    def test_empty_string(self):
        assert needs_update("") is True


# ---------------------------------------------------------------------------
# build_patch
# ---------------------------------------------------------------------------

class TestBuildPatch:

    def test_structure(self):
        patch = build_patch(0, "http://hdl.handle.net/20.500.12698/1785")
        assert len(patch) == 1
        op = patch[0]
        assert op["op"] == "replace"
        assert op["path"] == "/metadata/dc.identifier.uri/0/value"
        assert op["value"] == "http://hdl.handle.net/20.500.12698/1785"

    def test_index_in_path(self):
        patch = build_patch(3, "http://hdl.handle.net/123/456")
        assert patch[0]["path"] == "/metadata/dc.identifier.uri/3/value"

    def test_value_propagated(self):
        new_val = "http://hdl.handle.net/20.500.12698/99"
        patch = build_patch(1, new_val)
        assert patch[0]["value"] == new_val


# ---------------------------------------------------------------------------
# Integration-style: parse_handle -> build_handle_url round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:

    def test_typical_repository_url(self):
        original = "https://dspace.mendelu.cz/handle/20.500.12698/1785"
        handle = parse_handle(original)
        assert handle == "20.500.12698/1785"
        canonical = build_handle_url(handle)
        assert canonical == "http://hdl.handle.net/20.500.12698/1785"
        assert not needs_update(canonical)


# ---------------------------------------------------------------------------
# fixer – URL verification gate (no real network calls)
# ---------------------------------------------------------------------------

class TestFixerUrlVerification:

    def _make_item(self, uri_value: str) -> dict:
        return {
            "uuid": "test-uuid",
            "metadata": {"dc.identifier.uri": [{"value": uri_value}]},
        }

    def _make_dspace_be(self):
        be = mock.MagicMock()
        be.endpoint = "http://localhost/api"
        be.client.PatchOperation.REPLACE = "replace"
        be.client.api_patch.return_value = mock.MagicMock(ok=True)
        return be

    def test_valid_url_updates(self):
        item = self._make_item("https://dspace.mendelu.cz/handle/20.500.12698/1")
        be = self._make_dspace_be()
        f = fixer(be)
        with mock.patch.object(fixer, "_url_resolves", return_value=True):
            f.fix_item(item)
        assert f.stats["updated"] == 1
        assert f.invalid_handles == []

    def test_unresolvable_url_skipped(self):
        item = self._make_item("https://dspace.mendelu.cz/handle/20.500.12698/999")
        be = self._make_dspace_be()
        f = fixer(be)
        with mock.patch.object(fixer, "_url_resolves", return_value=False):
            f.fix_item(item)
        assert f.stats["updated"] == 0
        assert len(f.invalid_handles) == 1
        assert f.invalid_handles[0][0] == "test-uuid"
        be.client.api_patch.assert_not_called()

    def test_already_correct_not_touched(self):
        item = self._make_item("http://hdl.handle.net/20.500.12698/1")
        be = self._make_dspace_be()
        f = fixer(be)
        f.fix_item(item)
        assert f.stats["already_ok"] == 1
        assert f.stats["updated"] == 0
        be.client.api_patch.assert_not_called()
