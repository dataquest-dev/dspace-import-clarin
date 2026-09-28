"""Drive remove_authority.process() against a fake DSpace REST API.

The fake mirrors the two server behaviours the tool depends on: the GET collapses the
values of a field into a set keyed on `place`, while a PATCH indexes into the full,
uncollapsed list.
"""
import json
import os
import re
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from remove_authority_tool import AUTHOR, tool, value  # noqa
import dspace  # noqa

ITEMS = {}
PATCHED = []
BEHAVIOUR = {}
SERVER = None
BASE = None


def add_item(handle, authors, extra=None):
    uuid = f"{len(ITEMS):08d}-0000-0000-0000-000000000000"
    metadata = {"dc.title": [value("Title")]}
    if authors is not None:
        metadata[AUTHOR] = authors
    metadata.update(extra or {})
    ITEMS[uuid] = {"handle": handle, "metadata": metadata, "type": "item"}
    return uuid


def rest_view(metadata):
    """MetadataConverter: a TreeSet on `place`, the first value of a place wins."""
    view = {}
    for field, values in metadata.items():
        kept = {}
        for val in sorted(values, key=lambda x: x["place"]):
            kept.setdefault(val["place"], val)
        view[field] = list(kept.values())
    return view


def item_json(uuid):
    stored = ITEMS[uuid]
    return {"uuid": uuid, "handle": stored["handle"], "type": stored["type"],
            "metadata": rest_view(stored["metadata"])}


class Handler(BaseHTTPRequestHandler):

    def log_message(self, *args):
        pass

    def _send(self, code, payload=None, headers=None):
        body = json.dumps(payload).encode() if payload is not None else b""
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, val in (headers or {}).items():
            self.send_header(key, val)
        self.end_headers()
        self.wfile.write(body)

    def _uuid_of(self, handle):
        for uuid, stored in ITEMS.items():
            if stored["handle"] == handle:
                return uuid
        return None

    def do_POST(self):
        self._send(200, {}, {"Authorization": "Bearer token"})

    def do_GET(self):
        if "authn/status" in self.path:
            self._send(200, {"authenticated": True})
            return
        if self.path.startswith("/server/api/pid/find"):
            wanted = unquote(parse_qs(urlparse(self.path).query)["id"][0])
            uuid = self._uuid_of(wanted.replace("hdl:", "", 1))
            if uuid is None:
                self._send(404, {})
                return
            self.send_response(302)
            self.send_header("Location", f"{BASE}/core/items/{uuid}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        match = re.match(r"/server/api/core/items/([0-9a-f-]+)$", self.path)
        if match and match.group(1) in ITEMS:
            self._send(200, item_json(match.group(1)))
            return
        self._send(404, {})

    def do_PUT(self):
        self._send(500, {"message": "the tool must never PUT a whole item"})

    def do_PATCH(self):
        match = re.match(r"/server/api/core/items/([0-9a-f-]+)$", self.path)
        if not match:
            self._send(404, {})
            return
        uuid = match.group(1)
        ops = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        PATCHED.append((uuid, ops))

        how = BEHAVIOUR.get(uuid)
        if how == "refuse":
            self._send(422, {"message": "no metadata of this type at that index"})
            return
        if how == "expired":
            BEHAVIOUR.pop(uuid)
            self._send(401, {"message": "token expired"})
            return
        if how == "ignore":
            ops = []

        for op in ops:
            _, _, field, index = op["path"].split("/")
            # like DSpaceObjectMetadataReplaceOperation: alter the existing value,
            # indexing the uncollapsed list, `place` untouched
            ITEMS[uuid]["metadata"][field][int(index)].update({
                "value": op["value"]["value"],
                "language": op["value"].get("language"),
                "authority": op["value"].get("authority"),
                "confidence": op["value"].get("confidence", -1),
            })
        self._send(200, item_json(uuid))


def setUpModule():
    global SERVER, BASE
    SERVER = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    BASE = f"http://127.0.0.1:{SERVER.server_address[1]}/server/api"
    threading.Thread(target=SERVER.serve_forever, daemon=True).start()


def tearDownModule():
    SERVER.shutdown()
    SERVER.server_close()


class RestTestCase(unittest.TestCase):

    def setUp(self):
        ITEMS.clear()
        PATCHED.clear()
        BEHAVIOUR.clear()
        self.backend = dspace.rest(BASE, "user", "password", True)
        self.addCleanup(self.backend.client.session.close)

    def authors(self, uuid):
        return [(x["value"], x["authority"], x["place"])
                for x in ITEMS[uuid]["metadata"][AUTHOR]]

    def run_tool(self, handle, field=AUTHOR, dry_run=False):
        return tool.process(self.backend, handle, field, dry_run)


class TestHappyPath(RestTestCase):

    def test_only_the_values_with_an_authority_are_patched(self):
        uuid = add_item("123456789/1", [
            value("Doe, John", "orcid-1", 600, 0),
            value("Roe, Jane", None, -1, 1),
            value("Poe, Ann", "", -1, 2),                 # empty is not an authority
            value("Loe, Max", "orcid-2", 500, 3),
            value("Virtual, V", "virtual::42", 600, 4),   # stays, verify() allows it
        ], extra={"dc.subject": [value("kw", "vocab-1", 600)]})

        self.assertEqual("updated", self.run_tool("123456789/1"))
        self.assertEqual([("Doe, John", None, 0), ("Roe, Jane", None, 1),
                          ("Poe, Ann", "", 2), ("Loe, Max", None, 3),
                          ("Virtual, V", "virtual::42", 4)], self.authors(uuid))
        self.assertEqual([("replace", f"/metadata/{AUTHOR}/0"),
                          ("replace", f"/metadata/{AUTHOR}/3")],
                         [(op["op"], op["path"]) for _, ops in PATCHED for op in ops])
        self.assertEqual("vocab-1",
                         ITEMS[uuid]["metadata"]["dc.subject"][0]["authority"])
        self.assertEqual("Title", ITEMS[uuid]["metadata"]["dc.title"][0]["value"])

    def test_the_whole_item_takes_one_request(self):
        add_item("123456789/1", [value("A, A", "orcid-1", 600, 0),
                                 value("B, B", "orcid-2", 600, 1),
                                 value("C, C", "orcid-3", 600, 2)])
        self.assertEqual("updated", self.run_tool("123456789/1"))
        self.assertEqual(1, len(PATCHED))
        self.assertEqual(3, len(PATCHED[0][1]))

    def test_gapped_places_are_patched_by_position(self):
        uuid = add_item("123456789/1", [
            value("Gap, Zero", None, -1, 0),
            value("Gap, Five", "orcid-5", 600, 5),
            value("Gap, Nine", "orcid-9", 600, 9),
        ])

        self.assertEqual("updated", self.run_tool("123456789/1"))
        self.assertEqual([f"/metadata/{AUTHOR}/1", f"/metadata/{AUTHOR}/2"],
                         [op["path"] for _, ops in PATCHED for op in ops])
        self.assertEqual([("Gap, Zero", None, 0), ("Gap, Five", None, 5),
                          ("Gap, Nine", None, 9)], self.authors(uuid))

    def test_rerun_does_not_touch_a_clean_item(self):
        add_item("123456789/1", [value("Doe, John", "orcid-1", 600, 0)])
        self.assertEqual("updated", self.run_tool("123456789/1"))
        PATCHED.clear()
        self.assertEqual("skipped", self.run_tool("123456789/1"))
        self.assertEqual([], PATCHED)

    def test_another_field(self):
        uuid = add_item("123456789/1", [value("Doe, John", None, -1, 0)],
                        extra={"dc.contributor.editor": [
                            value("Ed, E", "orcid-e", 600, 0, "en")]})

        self.assertEqual("updated",
                         self.run_tool("123456789/1", field="dc.contributor.editor"))
        edited = ITEMS[uuid]["metadata"]["dc.contributor.editor"][0]
        self.assertEqual(("Ed, E", "en", None, -1),
                         (edited["value"], edited["language"],
                          edited["authority"], edited["confidence"]))

    def test_an_item_of_nothing_but_relationship_values(self):
        uuid = add_item("123456789/1", [value("Virtual, V", "virtual::42", 600, 0)])
        self.assertEqual("skipped", self.run_tool("123456789/1"))
        self.assertEqual([], PATCHED)
        self.assertEqual("virtual::42", ITEMS[uuid]["metadata"][AUTHOR][0]["authority"])

    def test_dry_run_writes_nothing(self):
        uuid = add_item("123456789/1", [value("Doe, John", "orcid-1", 600, 0)])
        self.assertEqual("updated", self.run_tool("123456789/1", dry_run=True))
        self.assertEqual([], PATCHED)
        self.assertEqual([("Doe, John", "orcid-1", 0)], self.authors(uuid))


class TestNothingToDo(RestTestCase):

    def test_no_authority(self):
        add_item("123456789/1", [value("Doe, John", None, -1, 0)])
        self.assertEqual("skipped", self.run_tool("123456789/1"))
        self.assertEqual([], PATCHED)

    def test_field_not_on_the_item(self):
        add_item("123456789/1", None)
        self.assertEqual("no field", self.run_tool("123456789/1"))


class TestFailures(RestTestCase):

    def test_unknown_handle(self):
        self.assertEqual("failed", self.run_tool("123456789/404"))

    def test_handle_of_something_that_is_not_an_item(self):
        uuid = add_item("123456789/9", [value("Doe, John", "orcid-1", 600, 0)])
        ITEMS[uuid]["type"] = "community"
        self.assertEqual("failed", self.run_tool("123456789/9"))
        self.assertEqual([], PATCHED)

    def test_refused_patch(self):
        uuid = add_item("123456789/1", [value("Doe, John", "orcid-1", 600, 0)])
        BEHAVIOUR[uuid] = "refuse"
        self.assertEqual("failed", self.run_tool("123456789/1"))

    def test_expired_token_is_retried(self):
        uuid = add_item("123456789/1", [value("Doe, John", "orcid-1", 600, 0)])
        BEHAVIOUR[uuid] = "expired"
        self.assertEqual("updated", self.run_tool("123456789/1"))
        self.assertEqual([("Doe, John", None, 0)], self.authors(uuid))

    def test_a_server_that_ignores_the_patch_stops_the_run(self):
        uuid = add_item("123456789/1", [value("Doe, John", "orcid-1", 600, 0)])
        BEHAVIOUR[uuid] = "ignore"
        with self.assertRaises(tool.UnexpectedState):
            self.run_tool("123456789/1")

    def test_two_values_sharing_a_place_stop_the_run(self):
        # the GET hides the second value of place 1, so the server-side index of
        # "D, Delta" is one further than the response makes it look
        uuid = add_item("123456789/1", [
            value("A, Alpha", None, -1, 0),
            value("B, Beta", None, -1, 1),
            value("C, Gamma", None, -1, 1),
            value("D, Delta", "orcid-D", 600, 2),
        ])
        with self.assertRaises(tool.UnexpectedState):
            self.run_tool("123456789/1")
        # the damage is real, which is why the run must not continue
        self.assertIn("D, Delta", [x[0] for x in self.authors(uuid)])
        self.assertNotIn("C, Gamma", [x[0] for x in self.authors(uuid)])


if __name__ == '__main__':
    unittest.main()
