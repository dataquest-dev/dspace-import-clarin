import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from remove_authority_tool import load_tool  # noqa

tool = load_tool()
AUTHOR = "dc.contributor.author"


def _value(value, authority=None, confidence=-1, place=0, language=None):
    return {"value": value, "language": language, "authority": authority,
            "confidence": confidence, "place": place}


class TestWithAuthority(unittest.TestCase):

    def test_only_values_with_an_authority(self):
        values = [
            _value("Doe, John", "orcid-1", 600, 0),
            _value("Roe, Jane", None, -1, 1),
            _value("Poe, Ann", "", -1, 2),
            _value("Loe, Max", "orcid-2", 500, 3),
        ]
        found = tool.with_authority(values)
        self.assertEqual([0, 3], [i for i, _ in found])
        self.assertEqual(["Doe, John", "Loe, Max"], [x["value"] for _, x in found])

    def test_index_is_the_position_not_the_place(self):
        # a migrated repository can have gaps in `place`, the patch path uses the position
        values = [
            _value("Doe, John", None, -1, 0),
            _value("Roe, Jane", "orcid-1", 600, 5),
            _value("Poe, Ann", "orcid-2", 600, 9),
        ]
        self.assertEqual([1, 2], [i for i, _ in tool.with_authority(values)])

    def test_relationship_values_are_left_alone(self):
        values = [_value("Virtual, V", "virtual::42", 600, 0),
                  _value("Real, R", "orcid-1", 600, 1)]
        self.assertEqual([1], [i for i, _ in tool.with_authority(values)])

    def test_nothing_to_do(self):
        self.assertEqual([], tool.with_authority([_value("Doe, John")]))


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
        self.assertEqual(1, len(client.calls))
        url, operation, path, value = client.calls[0]
        self.assertEqual(("http://x/api/core/items/u", "replace",
                          f"/metadata/{AUTHOR}/3"), (url, operation, path))
        self.assertEqual({"value": "Doe, John", "language": "en",
                          "authority": None, "confidence": -1}, value)


class TestVerify(unittest.TestCase):

    def _item(self, values):
        return {"metadata": {AUTHOR: values}}

    def test_clean_item_passes(self):
        tool.verify(AUTHOR, ["A", "B"],
                    self._item([_value("A"), _value("B", "virtual::7", 600, 1)]))

    def test_a_changed_value_stops_the_run(self):
        with self.assertRaises(tool.UnexpectedState):
            tool.verify(AUTHOR, ["A", "B"], self._item([_value("A"), _value("C")]))

    def test_a_surviving_authority_stops_the_run(self):
        with self.assertRaises(tool.UnexpectedState):
            tool.verify(AUTHOR, ["A"], self._item([_value("A", "orcid-1", 600)]))


class TestRun(unittest.TestCase):

    def _fake_process(self, results):
        seen = []

        def fake(dspace_be, handle, field, dry_run):
            seen.append(handle)
            outcome = results[handle]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        self.seen = seen
        real = tool.process
        tool.process = fake
        self.addCleanup(setattr, tool, "process", real)

    def test_counts_every_outcome(self):
        self._fake_process({"h1": "updated", "h2": "skipped",
                            "h3": "no field", "h4": "failed"})
        counts, failed = tool.run(None, ["h1", "h2", "h3", "h4"], AUTHOR, False)
        self.assertEqual({"updated": 1, "skipped": 1, "no field": 1, "failed": 1},
                         counts)
        self.assertEqual(["h4"], failed)

    def test_a_broken_connection_does_not_end_the_run(self):
        self._fake_process({"h1": ConnectionError("reset"), "h2": "updated"})
        counts, failed = tool.run(None, ["h1", "h2"], AUTHOR, False)
        self.assertEqual(["h1", "h2"], self.seen)
        self.assertEqual(1, counts["updated"])
        self.assertEqual(["h1"], failed)

    def test_an_unexpected_state_stops_the_rest_of_the_run(self):
        self._fake_process({"h1": tool.UnexpectedState("values changed"),
                            "h2": "updated"})
        counts, failed = tool.run(None, ["h1", "h2"], AUTHOR, False)
        self.assertEqual(["h1"], self.seen)
        self.assertEqual(0, counts["updated"])
        self.assertEqual(["h1"], failed)


class TestLoadHandles(unittest.TestCase):

    def _write(self, content):
        with tempfile.NamedTemporaryFile("wb", suffix=".txt", delete=False) as fout:
            fout.write(content)
        self.addCleanup(os.unlink, fout.name)
        return fout.name

    def test_comments_blanks_and_prefixes(self):
        path = self._write("\n".join([
            "# a comment", "", "  123456789/1  ",
            "https://hdl.handle.net/123456789/2",
            "http://hdl.handle.net/123456789/3",
            "https://dspace.vsb.cz/handle/123456789/4",
        ]).encode("utf-8"))
        self.assertEqual(
            ["123456789/1", "123456789/2", "123456789/3", "123456789/4"],
            tool.load_handles(path))

    def test_file_saved_with_a_bom(self):
        path = self._write("123456789/1\n".encode("utf-8-sig"))
        self.assertEqual(["123456789/1"], tool.load_handles(path))


if __name__ == '__main__':
    unittest.main()
