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
    """
        --fix-date-format on dc.date.issued (dry-run, no DSpace): YYYY, YYYY-MM and
        YYYY-MM-DD are kept, other full dates go to YYYY-MM-DD, partial dates to
        YYYY-MM, empty and unparseable values are left untouched.
    """

    CASES = [
        (None, ANOMALY),
        ("", ANOMALY),
        ("   ", ANOMALY),
        ("2020", KEEP),
        ("2020-05-30", KEEP),
        ("2025-05", KEEP),
        ("2020-5-3", "2020-05-03"),
        ("2025-5", "2025-05"),
        ("30.5.2025", "2025-05-30"),
        ("30. 5. 2025", "2025-05-30"),
        ("5.6.2025", "2025-06-05"),
        ("30/05/2025", "2025-05-30"),
        ("05/2025", "2025-05"),
        ("5/2025", "2025-05"),
        ("5.2025", "2025-05"),
        ("2025/05", "2025-05"),
        ("2025.05", "2025-05"),
        ("5.30.2025", ANOMALY),
        ("abcd-ef", ANOMALY),
        ("2025-13", ANOMALY),
        ("31.2.2025", ANOMALY),
        ("2025-05-30T00:00:00Z", ANOMALY),
        ("ca. 2020", ANOMALY),
        ("[2020]", ANOMALY),
        ("2020-2021", ANOMALY),
    ]

    def _run(self, value):
        item = {"uuid": "u", "metadata": {"dc.date.issued": [{"value": value}]}}
        upd = _am.updater(None, ["dc.date.issued"], "dc.date.issued", dry_run=True)
        ret = upd.update(item)
        return ret, item["metadata"]["dc.date.issued"][0]["value"]

    def test_cases(self):
        for value, expected in self.CASES:
            with self.subTest(value=value):
                ret, stored = self._run(value)
                if expected == KEEP:
                    self.assertEqual(ret, _am.updater.ret_already_ok)
                    self.assertEqual(stored, value)
                elif expected == ANOMALY:
                    self.assertEqual(ret, _am.updater.ret_invalid_meta)
                    self.assertEqual(stored, value)
                else:
                    self.assertEqual(ret, _am.updater.ret_updated)
                    self.assertEqual(stored, expected)


if __name__ == "__main__":
    unittest.main()
