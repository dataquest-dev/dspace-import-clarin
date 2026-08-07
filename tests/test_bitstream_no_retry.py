import os
import sys
import types
import unittest
import importlib.util

import requests

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LIB_DIR = os.path.join(ROOT_DIR, "libs", "dspace-rest-python")
if LIB_DIR not in sys.path:
    sys.path.insert(0, LIB_DIR)


def _load_rest():
    """Load src/dspace/_rest.py without importing the whole dspace package."""
    pkg_dir = os.path.join(ROOT_DIR, "src", "dspace")
    if "dspace" not in sys.modules:
        pkg = types.ModuleType("dspace")
        pkg.__path__ = [pkg_dir]
        sys.modules["dspace"] = pkg
    spec = importlib.util.spec_from_file_location(
        "dspace._rest", os.path.join(pkg_dir, "_rest.py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["dspace._rest"] = module
    spec.loader.exec_module(module)
    return module


_rest = _load_rest()


class _Resp:
    def __init__(self, code):
        self.status_code = code
        self.ok = False
        self.content = b""


class TestBitstreamPostNotRepeated(unittest.TestCase):
    """
        The bitstream import endpoint is not idempotent: the server commits the
        new bundle2bitstream row even after the client's read timeout, so a
        second POST creates a duplicate row with a colliding bitstream_order
        and shadows a file (dataquest-dev/dspace-customers#878).
    """

    def _rest_instance(self):
        r = object.__new__(_rest.rest)
        r.calls = []
        r._is_circuit_breaker_open = lambda: False
        r._handle_circuit_breaker = lambda code: None
        r._maybe_reauthenticate = lambda force=False: True
        return r

    def _raising(self, exc):
        r = self._rest_instance()

        def post(command, params=None, data=None):
            r.calls.append(command)
            raise exc

        r.post = post
        return r

    def _returning(self, status_code):
        r = self._rest_instance()
        r.forced_reauths = 0

        def reauth(force=False):
            if force:
                r.forced_reauths += 1
            return True

        def post(command, params=None, data=None):
            r.calls.append(command)
            return _Resp(status_code)

        r._maybe_reauthenticate = reauth
        r.post = post
        return r

    def setUp(self):
        self._sleep = _rest.time.sleep
        _rest.time.sleep = lambda s: None

    def tearDown(self):
        _rest.time.sleep = self._sleep

    def test_read_timeout_does_not_repeat_the_post(self):
        r = self._raising(requests.exceptions.ReadTimeout("Read timed out."))
        result = r.put_bitstream({"internal_id": "abc"}, {"name": "big.zip"})

        self.assertIsNone(result)
        self.assertEqual(r.calls, [_rest.BITSTREAM_IMPORT_URL],
                         "bitstream POST must not be repeated after a read timeout")

    def test_ambiguous_5xx_does_not_repeat_the_post(self):
        for code in _rest.HTTP_AMBIGUOUS_FAILURE_CODES:
            r = self._returning(code)
            r.put_bitstream({"internal_id": "abc"}, {"name": "big.zip"})
            self.assertEqual(len(r.calls), 1, f"HTTP {code} must not be retried")

    def test_rate_limit_is_still_retried(self):
        # 408/429 are rejected before the controller runs - no duplicate is
        # possible, so losing the bitstream over one blip would be a regression.
        for code in [408, 429]:
            r = self._returning(code)
            r.put_bitstream({"internal_id": "abc"}, {"name": "big.zip"})
            self.assertEqual(len(r.calls), _rest.HTTP_MAX_RETRIES,
                             f"HTTP {code} must keep its retries")

    def test_expired_session_still_retries_and_reauthenticates(self):
        # A multi-hour import outlives its token. 401/403 are rejected by
        # @PreAuthorize("hasAuthority('ADMIN')") before the controller body
        # runs, so nothing is created and retrying cannot duplicate a row -
        # unlike a read timeout, where the server keeps working and commits.
        for code in [401, 403]:
            r = self._returning(code)
            r.put_bitstream({"internal_id": "abc"}, {"name": "big.zip"})
            self.assertEqual(len(r.calls), _rest.HTTP_MAX_RETRIES,
                             f"HTTP {code} must keep its retries")
            self.assertGreater(r.forced_reauths, 0,
                               f"HTTP {code} must trigger re-authentication")

    def test_other_endpoints_still_retry(self):
        r = self._raising(requests.exceptions.ReadTimeout("Read timed out."))
        list(r._iput("clarin/import/core/item", [{}], [{}]))

        self.assertEqual(len(r.calls), _rest.HTTP_MAX_RETRIES,
                         "idempotent endpoints must keep their retries")

    def test_bitstream_endpoint_gets_the_long_read_timeout(self):
        base = "http://dev-5.pc:88/repository/server/api/"
        long_t = (_rest.HTTP_CONNECT_TIMEOUT, _rest.HTTP_READ_TIMEOUT_BITSTREAM)
        short_t = (_rest.HTTP_CONNECT_TIMEOUT, _rest.HTTP_READ_TIMEOUT)

        self.assertEqual(_rest.rest._timeout_for(base + "clarin/import/core/bitstream"), long_t)
        self.assertEqual(_rest.rest._timeout_for(base + "clarin/import/core/bitstream/checksum"), long_t)
        self.assertEqual(_rest.rest._timeout_for(base + "clarin/import/core/item"), short_t)
        self.assertGreater(_rest.HTTP_READ_TIMEOUT_BITSTREAM, _rest.HTTP_READ_TIMEOUT)


if __name__ == "__main__":
    unittest.main()
