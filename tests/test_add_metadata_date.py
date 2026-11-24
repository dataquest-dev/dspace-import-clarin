import importlib.util
import logging
import os
import sys
import types
import unittest

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load_add_metadata():
    """Load tools/add_metadata/add_metadata.py with stubbed DSpace/env dependencies."""
    utils = types.ModuleType("utils")
    utils.load_env = lambda *a, **k: None
    utils.init_logging = lambda *a, **k: None
    utils.update_settings = lambda *a: {
        "log_file": None, "backend": {"endpoint": "", "user": "", "password": ""}}
    models = types.ModuleType("dspace_rest_client.models")
    models.Item = dict
    stubs = {
        "utils": utils,
        "dspace": types.ModuleType("dspace"),
        "settings": types.SimpleNamespace(env={}),
        "project_settings": types.SimpleNamespace(settings={}),
        "dspace_rest_client": types.ModuleType("dspace_rest_client"),
        "dspace_rest_client.models": models,
    }
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)
    try:
        path = os.path.join(ROOT_DIR, "tools", "add_metadata", "add_metadata.py")
        spec = importlib.util.spec_from_file_location("add_metadata", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    return module


_am = _load_add_metadata()
logging.getLogger().setLevel(logging.CRITICAL)

KEEP = "keep"
ANOMALY = "anomaly"


class TestFixDateFormat(unittest.TestCase):
    """--fix-date-format on dc.date.issued, as used at TUL (dry-run, no DSpace)."""

    CASES = [
        (None, KEEP),
        ("", KEEP),
        ("   ", KEEP),
        ("2020", KEEP),
        ("2025-05", KEEP),
        ("2020-05-30", KEEP),
        ("2020-5-3", KEEP),
        ("30.5.2025", "2025-05-30"),
        ("30. 5. 2025", "2025-05-30"),
        ("30/05/2025", "2025-05-30"),
        ("30-05-2025", "2025-05-30"),
        ("5/2025", "2025-05"),
        ("5.2025", "2025-05"),
        ("2025-5", "2025-05"),
        ("31.2.2025", ANOMALY),
        ("2025-06-18T00:00:00Z", ANOMALY),
        ("ca. 2020", ANOMALY),
    ]

    def _run(self, value):
        item = {"uuid": "u", "metadata": {"dc.date.issued": [{"value": value}]}}
        upd = _am.updater(None, ["dc.date.issued"], "dc.date.issued",
                          dry_run=True, fix_date_format=True)
        ret = upd.update(item)
        return ret, item["metadata"]["dc.date.issued"][0]["value"], upd.info

    def test_cases(self):
        for value, expected in self.CASES:
            with self.subTest(value=value):
                ret, stored, info = self._run(value)
                if expected == KEEP:
                    self.assertEqual((ret, stored), (_am.updater.ret_already_ok, value))
                elif expected == ANOMALY:
                    self.assertEqual((ret, stored), (_am.updater.ret_invalid_meta, value))
                    self.assertEqual(info["invalid_date"], [("u", value)])
                else:
                    self.assertEqual((ret, stored), (_am.updater.ret_updated, expected))
                    # a second run must not write the fixed value again
                    self.assertEqual(self._run(stored)[0], _am.updater.ret_already_ok)


class TestMultipleValues(unittest.TestCase):
    """Extra dc.date.issued values are never deleted, in any mode."""

    def test_nothing_removed(self):
        values = [{"value": "abc"}, {"value": "def"}, {"value": "2020"}]
        for fix in (False, True):
            with self.subTest(fix_date_format=fix):
                item = {"uuid": "u", "metadata": {"dc.date.issued": list(values)}}
                # dspace_be=None and dry_run=False: any DSpace call would raise
                upd = _am.updater(None, ["dc.date.submitted"], "dc.date.issued",
                                  fix_date_format=fix)
                self.assertEqual(upd.update(item), _am.updater.ret_invalid_meta)
                self.assertEqual(item["metadata"]["dc.date.issued"], values)
                self.assertEqual(upd.info["multiple"], {"u"})


if __name__ == "__main__":
    unittest.main()
