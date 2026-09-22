import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from remove_authority_tool import AUTHOR, tool, value  # noqa


class TestAuthorityPatchOps(unittest.TestCase):

    def test_one_replace_per_value_without_the_authority(self):
        to_clear = [(0, value("Doe, John", "orcid-1", 600, 0, "en")),
                    (3, value("Loe, Max", "orcid-2", 500, 3))]
        self.assertEqual([
            {"op": "replace", "path": f"/metadata/{AUTHOR}/0",
             "value": {"value": "Doe, John", "language": "en",
                       "authority": None, "confidence": -1}},
            {"op": "replace", "path": f"/metadata/{AUTHOR}/3",
             "value": {"value": "Loe, Max", "language": None,
                       "authority": None, "confidence": -1}},
        ], tool.authority_patch_ops(AUTHOR, to_clear))


class TestVerify(unittest.TestCase):

    def test_a_changed_value_stops_the_run(self):
        # a patch that lands on the wrong index can overwrite a value that had no
        # authority, and then nothing but this check notices
        with self.assertRaises(tool.UnexpectedState):
            tool.verify(AUTHOR, ["A", "B"],
                        {"metadata": {AUTHOR: [value("A"), value("A", None, -1, 1)]}})


class TestRun(unittest.TestCase):

    def _fake_process(self, results):
        self.seen = []

        def fake(dspace_be, handle, field, dry_run):
            self.seen.append(handle)
            outcome = results[handle]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

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

    def test_comments_blanks_and_urls(self):
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
