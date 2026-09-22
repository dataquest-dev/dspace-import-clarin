import importlib.util
import os
import sys
import tempfile
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOOL_FILE = os.path.join(ROOT_DIR, "tools", "remove_authority", "remove_authority.py")


def _load_tool():
    """Import the tool by path - it is not part of an importable package."""
    spec = importlib.util.spec_from_file_location("remove_authority", TOOL_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["remove_authority"] = module
    spec.loader.exec_module(module)
    return module


tool = _load_tool()
AUTHOR = "dc.contributor.author"


def _value(value, authority=None, confidence=-1, place=0, language=None):
    return {"value": value, "language": language, "authority": authority,
            "confidence": confidence, "place": place}


class TestWithAuthority(unittest.TestCase):

    def test_only_values_with_an_authority(self):
        item = {"metadata": {AUTHOR: [
            _value("Doe, John", "orcid-1", 600, 0),
            _value("Roe, Jane", None, -1, 1),
            _value("Poe, Ann", "", -1, 2),
            _value("Loe, Max", "orcid-2", 500, 3),
        ]}}
        found = tool.with_authority(item, AUTHOR)
        self.assertEqual([0, 3], [i for i, _ in found])
        self.assertEqual(["Doe, John", "Loe, Max"], [x["value"] for _, x in found])

    def test_index_is_the_position_not_the_place(self):
        # a migrated repository can have gaps in `place`, the patch path uses the position
        item = {"metadata": {AUTHOR: [
            _value("Doe, John", None, -1, 0),
            _value("Roe, Jane", "orcid-1", 600, 5),
            _value("Poe, Ann", "orcid-2", 600, 9),
        ]}}
        self.assertEqual([1, 2], [i for i, _ in tool.with_authority(item, AUTHOR)])

    def test_nothing_to_do(self):
        item = {"metadata": {AUTHOR: [_value("Doe, John")]}}
        self.assertEqual([], tool.with_authority(item, AUTHOR))

    def test_missing_field(self):
        self.assertEqual([], tool.with_authority({"metadata": {}}, AUTHOR))


class TestPatchAuthorityAway(unittest.TestCase):

    class _client:
        def __init__(self):
            self.calls = []

        def api_patch(self, url, operation, path, value):
            self.calls.append((url, operation, path, value))
            return None

    class _backend:
        def __init__(self, client):
            self.client = client

    def test_patch_keeps_the_value_and_drops_the_authority(self):
        client = self._client()
        tool.patch_authority_away(self._backend(client), "http://x/api/core/items/u",
                                  AUTHOR, 3, _value("Doe, John", "orcid-1", 600, 3, "en"))
        url, operation, path, value = client.calls[0]
        self.assertEqual(("http://x/api/core/items/u", "replace",
                          f"/metadata/{AUTHOR}/3"), (url, operation, path))
        self.assertEqual({"value": "Doe, John", "language": "en",
                          "authority": None, "confidence": -1}, value)


class TestLoadHandles(unittest.TestCase):

    def test_comments_blanks_and_prefixes(self):
        content = "\n".join([
            "# a comment", "", "  123456789/1  ",
            "https://hdl.handle.net/123456789/2",
            "http://hdl.handle.net/123456789/3",
        ])
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False,
                                         encoding="utf-8") as fout:
            fout.write(content)
        try:
            self.assertEqual(["123456789/1", "123456789/2", "123456789/3"],
                             tool.load_handles(fout.name))
        finally:
            os.unlink(fout.name)


if __name__ == '__main__':
    unittest.main()
