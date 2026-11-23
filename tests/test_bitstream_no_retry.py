import inspect
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

IMPORT_URL = "clarin/import/core/bitstream"


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
        r._bitstream_import_url = IMPORT_URL
        r._bitstream_read_timeout = 3600
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
        self.assertEqual(r.calls, [IMPORT_URL],
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

    def test_connect_timeout_is_still_retried(self):
        # The connection never came up, so no bytes left the client and the
        # server cannot have committed anything - giving up here would lose the
        # bitstream for nothing.
        r = self._raising(requests.exceptions.ConnectTimeout("Connection timed out."))
        r.put_bitstream({"internal_id": "abc"}, {"name": "big.zip"})

        self.assertEqual(len(r.calls), _rest.HTTP_MAX_RETRIES,
                         "a failed connect must keep its retries")

    def test_auth_failure_before_the_post_is_still_retried(self):
        # _maybe_reauthenticate() runs inside the same try, so a hiccup there
        # used to abandon the bitstream without a single POST being attempted.
        r = self._rest_instance()
        reauths = []

        def reauth(force=False):
            reauths.append(force)
            raise requests.exceptions.ConnectionError("auth endpoint down")

        def post(command, params=None, data=None):
            r.calls.append(command)
            return _Resp(200)

        r._maybe_reauthenticate = reauth
        r.post = post
        r.put_bitstream({"internal_id": "abc"}, {"name": "big.zip"})

        self.assertEqual(r.calls, [], "the POST was never attempted")
        self.assertEqual(len(reauths), _rest.HTTP_MAX_RETRIES,
                         "a pre-POST failure must keep its retries")

    def test_other_endpoints_still_retry(self):
        r = self._raising(requests.exceptions.ReadTimeout("Read timed out."))
        list(r._iput("clarin/import/core/item", [{}], [{}]))

        self.assertEqual(len(r.calls), _rest.HTTP_MAX_RETRIES,
                         "idempotent endpoints must keep their retries")

    def test_add_checksums_posts_under_the_bitstream_prefix(self):
        # add_checksums() must post under the same prefix _timeout_for() matches
        # on, otherwise it silently falls back to the 120s default while
        # re-hashing a whole batch.
        r = self._rest_instance()
        seen = []

        def post(command, params=None, data=None):
            seen.append(command)
            resp = _Resp(200)
            resp.ok = True
            return resp

        r.post = post
        _rest.rest.add_checksums(r)

        self.assertEqual(seen, [f"{IMPORT_URL}/checksum"])
        self.assertEqual(r._timeout_for("http://h/api/" + seen[0]),
                         (_rest.HTTP_CONNECT_TIMEOUT, 3600))

    def test_bitstream_endpoint_gets_the_configured_read_timeout(self):
        base = "http://dev-5.pc:88/repository/server/api/"
        r = self._rest_instance()
        r._bitstream_read_timeout = 7200
        long_t = (_rest.HTTP_CONNECT_TIMEOUT, 7200)
        short_t = (_rest.HTTP_CONNECT_TIMEOUT, _rest.HTTP_READ_TIMEOUT)

        self.assertEqual(r._timeout_for(base + "clarin/import/core/bitstream"), long_t)
        self.assertEqual(r._timeout_for(base + "clarin/import/core/bitstream/checksum"), long_t)
        self.assertEqual(r._timeout_for(base + "clarin/import/core/item"), short_t)


class TestBitstreamEndpointIsAMemberVariable(unittest.TestCase):
    """
        The import path is `self._bitstream_import_url`, settable from the
        constructor - the module constant is only its default. Every site that
        used the constant must read the member, otherwise a client constructed
        with a different path would post to one endpoint and time it as another.
    """

    def _client(self, url):
        r = object.__new__(_rest.rest)
        r._bitstream_import_url = url
        r._bitstream_read_timeout = 7200
        return r

    def test_ctor_default_is_the_server_endpoint(self):
        self.assertEqual(
            inspect.signature(
                _rest.rest.__init__).parameters["bitstream_import_url"].default,
            IMPORT_URL)

    def test_put_bitstream_posts_to_the_configured_path(self):
        r = self._client("some/other/bitstream")
        seen = []
        r._iput = lambda url, data, params, **kw: seen.append(url) or iter([None])

        _rest.rest.put_bitstream(r, {}, {})

        self.assertEqual(seen, ["some/other/bitstream"])

    def test_add_checksums_follows_the_configured_path(self):
        r = self._client("some/other/bitstream")
        seen = []

        def post(command, params=None, data=None):
            seen.append(command)
            resp = _Resp(200)
            resp.ok = True
            return resp

        r.post = post
        _rest.rest.add_checksums(r)

        self.assertEqual(seen, ["some/other/bitstream/checksum"])

    def test_timeout_follows_the_configured_path(self):
        r = self._client("some/other/bitstream")

        self.assertEqual(r._timeout_for("http://h/api/some/other/bitstream"),
                         (_rest.HTTP_CONNECT_TIMEOUT, 7200))
        # the old hardcoded path must no longer win the long timeout
        self.assertEqual(r._timeout_for("http://h/api/" + IMPORT_URL),
                         (_rest.HTTP_CONNECT_TIMEOUT, _rest.HTTP_READ_TIMEOUT))


class TestBitstreamReadTimeoutIsConfigurable(unittest.TestCase):
    """
        The timeout is a constructor argument fed from
        project_settings["backend"]["bitstream_read_timeout"], like
        reauth_minutes - not a module global, so it stays overridable
        via `--config backend.bitstream_read_timeout=...`.
    """

    def test_default_matches_project_settings(self):
        src_dir = os.path.join(ROOT_DIR, "src")
        if src_dir not in sys.path:
            sys.path.insert(0, src_dir)
        import project_settings

        configured = project_settings.settings["backend"]["bitstream_read_timeout"]
        ctor_default = inspect.signature(
            _rest.rest.__init__).parameters["bitstream_read_timeout"].default

        self.assertEqual(ctor_default, configured)
        self.assertGreater(configured, _rest.HTTP_READ_TIMEOUT)

    def test_repo_import_passes_the_setting_to_the_constructor(self):
        # A default that never reaches the client is worse than no default:
        # the import would silently keep the 120s timeout that caused #878.
        with open(os.path.join(ROOT_DIR, "src", "repo_import.py"), encoding="utf-8") as fin:
            source = fin.read()

        self.assertIn('env["backend"].get("bitstream_read_timeout"', source)

    def test_worker_clients_inherit_the_bitstream_settings(self):
        # Bitstreams are imported by spawned workers, so settings that stop at
        # the main client would not reach the requests that need them.
        recorded = []
        r = object.__new__(_rest.rest)
        r.endpoint = "http://h/api"
        r._user, r._password, r._auth = "u", "p", False
        r._reauth_minutes, r._bitstream_read_timeout = 20, 7200
        r._bitstream_import_url = "some/other/bitstream"

        original = _rest.rest.__init__
        _rest.rest.__init__ = lambda self, *a, **kw: recorded.append((a, kw))
        try:
            _rest.rest.spawn_worker_client(r)
        finally:
            _rest.rest.__init__ = original

        self.assertEqual(len(recorded), 1)
        args, kwargs = recorded[0]
        passed = list(args) + list(kwargs.values())
        self.assertIn(7200, passed)
        self.assertIn("some/other/bitstream", passed)


if __name__ == "__main__":
    unittest.main()
