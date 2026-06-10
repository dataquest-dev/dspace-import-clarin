import json
import os
import tempfile
import unittest
import sys
import types
import importlib.util


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LIB_DIR = os.path.join(ROOT_DIR, "libs", "dspace-rest-python")
if LIB_DIR not in sys.path:
    sys.path.insert(0, LIB_DIR)


def _load_module(module_name, file_path, package_name, package_path):
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [package_path]
        sys.modules[package_name] = package

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


rest = _load_module(
    "dspace._rest",
    os.path.join(ROOT_DIR, "src", "dspace", "_rest.py"),
    "dspace",
    os.path.join(ROOT_DIR, "src", "dspace"),
).rest

bitstreams = _load_module(
    "pump._bitstream",
    os.path.join(ROOT_DIR, "src", "pump", "_bitstream.py"),
    "pump",
    os.path.join(ROOT_DIR, "src", "pump"),
).bitstreams


class _StubMetadatas:
    def value(self, _rtype, _rid, log_missing=True):
        return {
            "dc.title": [
                {
                    "value": "sample-audio.flac",
                    "language": None,
                    "authority": None,
                    "confidence": -1,
                }
            ]
        }

    def filter_res_d(self, data, _ignored_fields):
        return data


class _StubBitstreamFormatRegistry:
    unknown_format_id = 1

    def mimetype(self, _format_id):
        return "audio/flac"


class _StubBundles:
    primary = {}

    def uuid(self, _bundle_id):
        return "bundle-uuid-1"


class _StubCommunities:
    logos = {}


class _StubCollections:
    logos = {}


def _write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


class TestBitstreamRecovery(unittest.TestCase):
    def test_put_bitstream_disables_transient_retry(self):
        r = rest.__new__(rest)
        captured = {}

        def _fake_iput(url, arr, params=None, allow_transient_retry=True):
            captured["url"] = url
            captured["allow_transient_retry"] = allow_transient_retry
            return iter([{"id": "uuid-123"}])

        r._iput = _fake_iput

        resp = r.put_bitstream({"sequenceId": 1}, {"metadata": {}})

        self.assertEqual(resp["id"], "uuid-123")
        self.assertEqual(captured["url"], "clarin/import/core/bitstream")
        self.assertFalse(captured["allow_transient_retry"])

    def test_recovers_uuid_after_invalid_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            bitstream_json = os.path.join(tmp, "bitstream.json")
            bundle2bitstream_json = os.path.join(tmp, "bundle2bitstream.json")
            cache_json = os.path.join(tmp, "cache.json")

            _write_json(
                bitstream_json,
                [
                    {
                        "bitstream_id": 1,
                        "deleted": False,
                        "size_bytes": 123,
                        "checksum_algorithm": "MD5",
                        "checksum": "abc123",
                        "bitstream_format_id": 1,
                        "internal_id": "00112233445566778899",
                        "store_number": 0,
                        "sequence_id": 3,
                    }
                ],
            )
            _write_json(bundle2bitstream_json, [{"bitstream_id": 1, "bundle_id": 11}])

            entity = bitstreams(bitstream_json, bundle2bitstream_json)

            class _StubDspace:
                def __init__(self):
                    self.fetch_calls = 0
                    self.put_calls = 0

                def put_bitstream(self, _params, _data):
                    self.put_calls += 1
                    return None

                def fetch_bundle_bitstreams(self, _bundle_uuid):
                    self.fetch_calls += 1
                    if self.fetch_calls == 1:
                        return []
                    return [
                        {
                            "id": "uuid-recovered-1",
                            "sequenceId": 3,
                            "name": "sample-audio.flac",
                            "checkSum": {"value": "abc123"},
                        }
                    ]

                def add_checksums(self):
                    return None

            dspace = _StubDspace()

            env = {
                "backend": {
                    "ignore_deleted_bitstreams": False,
                    "testing": False,
                },
                "assetstore": "",
            }

            entity._bitstream_import_to(
                env,
                cache_json,
                dspace,
                _StubMetadatas(),
                _StubBitstreamFormatRegistry(),
                _StubBundles(),
                _StubCommunities(),
                _StubCollections(),
            )

            self.assertEqual(entity.uuid(1), "uuid-recovered-1")
            self.assertEqual(entity.imported, 1)
            self.assertEqual(dspace.put_calls, 1)
            self.assertEqual(dspace.fetch_calls, 2)


if __name__ == "__main__":
    unittest.main()
