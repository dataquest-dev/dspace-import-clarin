"""
Unit tests for update_author_orcid helper functions.
No API calls are made – all tests operate on pure data.
"""
import os
import sys
import unittest.mock as mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../update_author_orcid"))

for mod_name in ("dspace", "settings", "project_settings", "utils", "tqdm"):
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock.MagicMock()

sys.modules["project_settings"].settings = {
    "log_file": os.path.join(os.path.dirname(__file__), "__test.log"),
    "backend": {"endpoint": "http://localhost/api", "user": "test", "password": "test"},
}
sys.modules["settings"].env = {"dspace": {"handle_prefix": ["http://hdl.handle.net/"], "orcid_url": "https://orcid.org/{}"}}
sys.modules["utils"].update_settings = lambda a, b: {
    "log_file": os.path.join(os.path.dirname(__file__), "__test.log"),
    "backend": {"endpoint": "http://localhost/api", "user": "test", "password": "test"},
    "dspace": {"handle_prefix": ["http://hdl.handle.net/"], "orcid_url": "https://orcid.org/{}"},
}
sys.modules["utils"].init_logging = lambda *a, **kw: None
sys.modules["utils"].load_env = lambda *a, **kw: None

from update_author_orcid import (  # noqa: E402
    parse_orcid_entry,
    build_orcid_map,
    match_author,
    build_patch,
    updater,
)


# ---------------------------------------------------------------------------
# parse_orcid_entry
# ---------------------------------------------------------------------------

class TestParseOrcidEntry:

    def test_typical_entry(self):
        result = parse_orcid_entry("Blake, Thomas 1111-2222-3333-4444")
        assert result == ("Blake, Thomas", "1111-2222-3333-4444")

    def test_entry_with_spaces_in_name(self):
        result = parse_orcid_entry("Jimenez Jimenez, Ana Maria 2222-3333-4444-555X")
        assert result == ("Jimenez Jimenez, Ana Maria", "2222-3333-4444-555X")

    def test_orcid_with_x_checksum(self):
        result = parse_orcid_entry("Doe, John 3333-4444-5555-666X")
        assert result == ("Doe, John", "3333-4444-5555-666X")

    def test_no_orcid_returns_none(self):
        assert parse_orcid_entry("Blake, Thomas") is None

    def test_empty_string_returns_none(self):
        assert parse_orcid_entry("") is None

    def test_orcid_only_no_name(self):
        result = parse_orcid_entry("1111-2222-3333-4444")
        assert result == ("", "1111-2222-3333-4444")

    def test_extra_whitespace(self):
        result = parse_orcid_entry("  Reed, Oliver   4444-5555-6666-777X  ")
        assert result == ("Reed, Oliver", "4444-5555-6666-777X")


# ---------------------------------------------------------------------------
# build_orcid_map
# ---------------------------------------------------------------------------

class TestBuildOrcidMap:

    def test_single_entry(self):
        metadata = {
            "dc.identifier.orcid": [{"value": "Blake, Thomas 1111-2222-3333-4444"}]
        }
        assert build_orcid_map(metadata) == {"Blake, Thomas": "1111-2222-3333-4444"}

    def test_multiple_entries(self):
        metadata = {
            "dc.identifier.orcid": [
                {"value": "Reed, Oliver 4444-5555-6666-777X"},
                {"value": "Lane, Sophie 5555-6666-7777-8888"},
            ]
        }
        result = build_orcid_map(metadata)
        assert result == {
            "Reed, Oliver": "4444-5555-6666-777X",
            "Lane, Sophie": "5555-6666-7777-8888",
        }

    def test_missing_key_returns_empty(self):
        assert build_orcid_map({}) == {}

    def test_invalid_entry_skipped(self):
        metadata = {
            "dc.identifier.orcid": [
                {"value": "No ORCID here"},
                {"value": "Blake, Thomas 1111-2222-3333-4444"},
            ]
        }
        result = build_orcid_map(metadata)
        assert result == {"Blake, Thomas": "1111-2222-3333-4444"}

    def test_empty_value_skipped(self):
        metadata = {"dc.identifier.orcid": [{"value": ""}]}
        assert build_orcid_map(metadata) == {}


# ---------------------------------------------------------------------------
# match_author
# ---------------------------------------------------------------------------

class TestMatchAuthor:

    def test_known_author(self):
        orcid_map = {"Blake, Thomas": "1111-2222-3333-4444"}
        assert match_author("Blake, Thomas", orcid_map) == "1111-2222-3333-4444"

    def test_unknown_author_returns_none(self):
        orcid_map = {"Blake, Thomas": "1111-2222-3333-4444"}
        assert match_author("Stone, Claire", orcid_map) is None

    def test_empty_map(self):
        assert match_author("Blake, Thomas", {}) is None

    def test_case_sensitive(self):
        orcid_map = {"Blake, Thomas": "1111-2222-3333-4444"}
        assert match_author("blake, thomas", orcid_map) is None


# ---------------------------------------------------------------------------
# build_patch
# ---------------------------------------------------------------------------

class TestBuildPatch:

    def test_structure(self):
        patch = build_patch(0, "1111-2222-3333-4444")
        assert len(patch) == 1
        op = patch[0]
        assert op["op"] == "replace"
        assert op["path"] == "/metadata/dc.contributor.author/0/authority"
        assert op["value"] == "1111-2222-3333-4444"

    def test_index_in_path(self):
        patch = build_patch(3, "1111-2222-3333-4444")
        assert patch[0]["path"] == "/metadata/dc.contributor.author/3/authority"

    def test_value_propagated(self):
        orcid = "2222-3333-4444-555X"
        assert build_patch(1, orcid)[0]["value"] == orcid


# ---------------------------------------------------------------------------
# updater
# ---------------------------------------------------------------------------

class TestUpdater:

    def _make_item(self, authors, orcid_entries):
        return {
            "uuid": "item-uuid-1",
            "metadata": {
                "dc.contributor.author": [{"value": a, "authority": None} for a in authors],
                "dc.identifier.orcid": [{"value": v} for v in orcid_entries],
            },
        }

    def _make_dspace_be(self):
        be = mock.MagicMock()
        be.endpoint = "http://localhost/api"
        be.client.PatchOperation.REPLACE = "replace"
        be.client.api_patch.return_value = mock.MagicMock(ok=True)
        return be

    def test_author_gets_orcid(self):
        item = self._make_item(
            ["Blake, Thomas"],
            ["Blake, Thomas 1111-2222-3333-4444"],
        )
        be = self._make_dspace_be()
        u = updater(be)
        with mock.patch.object(updater, "_orcid_resolves", return_value=True):
            u.update_item(item)
        assert u.stats["updated"] == 1
        be.client.api_patch.assert_called_once()

    def test_author_without_orcid_skipped(self):
        item = self._make_item(
            ["Stone, Claire"],
            ["Blake, Thomas 1111-2222-3333-4444"],
        )
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["updated"] == 0
        be.client.api_patch.assert_not_called()

    def test_already_ok_not_patched(self):
        item = {
            "uuid": "item-uuid-2",
            "metadata": {
                "dc.contributor.author": [{"value": "Blake, Thomas", "authority": "1111-2222-3333-4444"}],
                "dc.identifier.orcid": [{"value": "Blake, Thomas 1111-2222-3333-4444"}],
            },
        }
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["already_ok"] == 1
        assert u.stats["updated"] == 0
        be.client.api_patch.assert_not_called()

    def test_dry_run_no_patch(self):
        item = self._make_item(
            ["Reed, Oliver"],
            ["Reed, Oliver 4444-5555-6666-777X"],
        )
        be = self._make_dspace_be()
        u = updater(be, dry_run=True)
        with mock.patch.object(updater, "_orcid_resolves", return_value=True):
            u.update_item(item)
        assert u.stats["updated"] == 1
        be.client.api_patch.assert_not_called()

    def test_patch_failure_counted(self):
        item = self._make_item(
            ["Blake, Thomas"],
            ["Blake, Thomas 1111-2222-3333-4444"],
        )
        be = self._make_dspace_be()
        be.client.api_patch.return_value = mock.MagicMock(ok=False)
        u = updater(be)
        with mock.patch.object(updater, "_orcid_resolves", return_value=True):
            u.update_item(item)
        assert u.stats["failed"] == 1
        assert u.stats["updated"] == 0

    def test_multiple_authors_partial_match(self):
        item = self._make_item(
            ["Blake, Thomas", "Stone, Claire", "Reed, Oliver"],
            [
                "Blake, Thomas 1111-2222-3333-4444",
                "Reed, Oliver 4444-5555-6666-777X",
            ],
        )
        be = self._make_dspace_be()
        u = updater(be)
        with mock.patch.object(updater, "_orcid_resolves", return_value=True):
            u.update_item(item)
        assert u.stats["updated"] == 2
        assert be.client.api_patch.call_count == 2

    def test_no_orcid_metadata_skipped(self):
        item = {
            "uuid": "item-uuid-3",
            "metadata": {
                "dc.contributor.author": [{"value": "Blake, Thomas", "authority": None}],
            },
        }
        be = self._make_dspace_be()
        u = updater(be)
        u.update_item(item)
        assert u.stats["updated"] == 0
        be.client.api_patch.assert_not_called()
