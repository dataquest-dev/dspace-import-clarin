import unittest


class _FakeBackend:
    """In-memory backend that simulates timeout/invalid-response behavior."""

    def __init__(self):
        self._next_id = 100
        self.created = []
        self._attempts = {}

    def _new_id(self):
        self._next_id += 1
        return str(self._next_id)

    def create_bitstream(self, signature):
        attempt = self._attempts.get(signature, 0) + 1
        self._attempts[signature] = attempt

        # Simulate server-side create + client timeout for both signatures on first attempt.
        if attempt == 1:
            created_id = self._new_id()
            self.created.append((signature, created_id))
            raise TimeoutError("Read timed out")

        # Signature A then returns invalid response (legacy importer fails to map it).
        if signature[0] == "makon-flac.1" and attempt == 2:
            return None

        # Signature B creates duplicate on retry and returns that UUID.
        created_id = self._new_id()
        self.created.append((signature, created_id))
        return {"id": created_id}

    def find_by_signature(self, signature):
        for sig, uuid in self.created:
            if sig == signature:
                return uuid
        return None

    def count_by_signature(self, signature):
        return sum(1 for sig, _ in self.created if sig == signature)


def _legacy_import_once(source_signatures, backend):
    """Replicates old behavior: retries non-idempotent create + no recovery lookup."""
    mapping = {}
    for source_id, signature in source_signatures.items():
        resp = None
        for _attempt in range(3):
            try:
                resp = backend.create_bitstream(signature)
            except Exception:
                continue
            break

        if isinstance(resp, dict) and "id" in resp:
            mapping[source_id] = resp["id"]
    return mapping


def _fixed_import_once(source_signatures, backend):
    """Replicates new behavior: no transient retry + lookup recovery by signature."""
    mapping = {}
    for source_id, signature in source_signatures.items():
        existing = backend.find_by_signature(signature)
        if existing is not None:
            mapping[source_id] = existing
            continue

        resp = None
        try:
            # Single attempt only for non-idempotent create.
            resp = backend.create_bitstream(signature)
        except Exception:
            pass

        if isinstance(resp, dict) and "id" in resp:
            mapping[source_id] = resp["id"]
            continue

        recovered = backend.find_by_signature(signature)
        if recovered is not None:
            mapping[source_id] = recovered

    return mapping


class TestIssue665Replication(unittest.TestCase):
    def test_replication_with_legacy_behavior(self):
        source = {
            863: ("makon-flac.1", "1", "c1"),
            864: ("makon-flac.3", "3", "c3"),
        }
        backend = _FakeBackend()

        mapping = _legacy_import_once(source, backend)

        # Missing mapping for one source bitstream.
        self.assertNotIn(863, mapping)
        self.assertIn(864, mapping)

        # Duplicate create for bitstream 864 signature.
        self.assertEqual(backend.count_by_signature(source[864]), 2)

    def test_fixed_behavior_prevents_missing_and_duplicate(self):
        source = {
            863: ("makon-flac.1", "1", "c1"),
            864: ("makon-flac.3", "3", "c3"),
        }
        backend = _FakeBackend()

        mapping = _fixed_import_once(source, backend)

        # Both source bitstreams are mapped by response or recovery lookup.
        self.assertIn(863, mapping)
        self.assertIn(864, mapping)

        # No duplicate create for either signature.
        self.assertEqual(backend.count_by_signature(source[863]), 1)
        self.assertEqual(backend.count_by_signature(source[864]), 1)


if __name__ == "__main__":
    unittest.main()
