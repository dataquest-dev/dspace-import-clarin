import argparse
import json
import logging
import os
import sys
import time

import tqdm

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../../src"))

import utils  # noqa

utils.load_env(os.path.join(_this_dir, os.environ.get("ENVFILE", "../.env")))

import dspace  # noqa
import settings  # noqa  (src/settings – imported before local dir is on path)
from utils import init_logging, update_settings  # noqa

sys.path.insert(0, os.path.join(_this_dir, ".."))
import mendelu_settings  # noqa

logging.getLogger("dspace.client").setLevel(logging.WARNING)
_logger = logging.getLogger()
env = update_settings(settings.env, mendelu_settings.settings)
init_logging(_logger, env["log_file"])

_MAPPING_FILE = env["update_type"]["mapping_file"]
_DEFAULT_MAPPING_ERROR_PREFIX = "Failed to load update type mapping file"
_DEFAULT_MAPPING_ENCODING = "utf-8"


class LazyMapping:
    def __init__(self, path: str, logger: logging.Logger):
        self._path = path
        self._logger = logger
        self._data: dict | None = None

    def _load(self) -> dict:
        if self._data is not None:
            return self._data
        try:
            with open(self._path, encoding=_DEFAULT_MAPPING_ENCODING) as f:
                self._data = json.load(f)
        except FileNotFoundError:
            self._logger.error(
                "%s: file not found: %s",
                _DEFAULT_MAPPING_ERROR_PREFIX,
                self._path,
            )
            sys.exit(1)
        except json.JSONDecodeError as exc:
            self._logger.error(
                "%s: invalid JSON in %s: %s",
                _DEFAULT_MAPPING_ERROR_PREFIX,
                self._path,
                exc,
            )
            sys.exit(1)
        return self._data

    def __getitem__(self, key):
        return self._load()[key]

    def get(self, key, default=None):
        return self._load().get(key, default)

    def __contains__(self, key):
        return key in self._load()

    def items(self):
        return self._load().items()

    def keys(self):
        return self._load().keys()

    def values(self):
        return self._load().values()

    def __iter__(self):
        return iter(self._load())

    def __len__(self):
        return len(self._load())


DEFAULT_MAPPING = LazyMapping(_MAPPING_FILE, _logger)


def map_type(value: str, mapping: dict):
    """Return the mapped value for *value*, or None if not in *mapping*."""
    return mapping.get(value)


def build_patch(index: int, value: str) -> list:
    return [{"op": "replace", "path": f"/metadata/dc.type/{index}/value", "value": value}]


def should_update(current: str, mapped) -> bool:
    """Return True if *mapped* is not None and differs from *current*."""
    return mapped is not None and current != mapped


class updater:

    def __init__(self, dspace_be, mapping: dict = None, dry_run: bool = False):
        self._dspace_be = dspace_be
        self._mapping = mapping if mapping is not None else DEFAULT_MAPPING
        self._mapping_values = set(self._mapping.values())
        self._dry_run = dry_run
        self._stats = {"already_ok": 0, "updated": 0, "failed": 0, "not_mapped": 0, "no_type": 0}

    @property
    def stats(self) -> dict:
        return self._stats

    def update_item(self, item: dict):
        uuid = item["uuid"]
        type_entries = item.get("metadata", {}).get("dc.type", [])

        if not type_entries:
            self._stats["no_type"] += 1
            _logger.debug(f"Item [{uuid}]: no dc.type – skipped")
            return

        for idx, entry in enumerate(type_entries):
            current = entry.get("value", "")
            mapped = map_type(current, self._mapping)

            if not should_update(current, mapped):
                if mapped is None and current in self._mapping_values:
                    # Value is already the mapped target (e.g. "Article")
                    self._stats["already_ok"] += 1
                    _logger.debug(f"Item [{uuid}]: dc.type[{idx}]=[{current}] already correct")
                elif mapped is None:
                    self._stats["not_mapped"] += 1
                    _logger.debug(f"Item [{uuid}]: dc.type[{idx}]=[{current}] not in mapping – skipped")
                else:
                    # mapped == current (source key happens to equal its own target)
                    self._stats["already_ok"] += 1
                    _logger.debug(f"Item [{uuid}]: dc.type[{idx}]=[{current}] already correct")
                continue

            _logger.info(
                f"Item [{uuid}]: dc.type[{idx}] [{current}] -> [{mapped}]"
                + (" [DRY RUN]" if self._dry_run else "")
            )

            if not self._dry_run:
                patch = build_patch(idx, mapped)
                url = f"{self._dspace_be.endpoint}/core/items/{uuid}"
                r = self._dspace_be.client.api_patch(
                    url, self._dspace_be.client.PatchOperation.REPLACE, patch[0]["path"], mapped
                )
                if r is None or not r.ok:
                    _logger.error(f"Item [{uuid}]: PATCH failed dc.type[{idx}] [{current}] -> [{mapped}]")
                    self._stats["failed"] += 1
                    continue

            self._stats["updated"] += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set dc.type for each item based on a default mapping (e.g. J_ČLÁNEK -> article)"
    )
    parser.add_argument("--server", type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user", type=str, default=env["backend"]["user"])
    parser.add_argument("--password", type=str, default=env["backend"]["password"])
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    _logger.info(f"Arguments: {args}")

    start = time.time()
    dspace_be = dspace.rest(args.server, args.user, args.password, True)

    _logger.info("Fetching and updating items...")

    def _filtered_items():
        for page in dspace_be.iter_items():
            for item in page:
                if not item["withdrawn"] and item["inArchive"]:
                    yield item

    upd = updater(dspace_be, dry_run=args.dry_run)
    total_items = 0
    for item in tqdm.tqdm(_filtered_items(), desc="Updating types", unit="item"):
        upd.update_item(item)
        total_items += 1

    _logger.info(40 * "=")
    s = upd.stats
    _logger.info(
        f"Total items: {total_items}  "
        f"already_ok={s['already_ok']}  updated={s['updated']}  "
        f"failed={s['failed']}  not_mapped={s['not_mapped']}  no_type={s['no_type']}"
    )
    took = time.time() - start
    _logger.info(f"Total time: {took:.2f} s [{time.strftime('%H:%M:%S', time.gmtime(took))}]")
