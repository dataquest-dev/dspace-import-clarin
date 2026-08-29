import argparse
import logging
import os
import re
import sys
import time
from typing import Dict

import requests
import tqdm

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../../src"))

import utils  # noqa

utils.load_env(os.path.join(_this_dir, os.environ.get("ENVFILE", "../.env")))

import dspace  # noqa
import settings  # noqa  (src/settings – imported before local dir is on path)
from utils import init_logging, update_settings, apply_env_backend  # noqa

import project_settings  # noqa

logging.getLogger("dspace.client").setLevel(logging.WARNING)
_logger = logging.getLogger()
env = update_settings(project_settings.settings, settings.env)
env = apply_env_backend(env)
init_logging(_logger, env["log_file"])

# env["dspace"]["handle_prefix"] is defined in project_settings
HANDLE_PREFIX = env["dspace"]["handle_prefix"]
_HANDLE_PATH_RE = re.compile(r"/handle/([^/?#\s]+/[^/?#\s]+)")
_HANDLE_GENERIC_RE = re.compile(r"([0-9]+/[0-9]+)")


def parse_handle(url: str):
    m = _HANDLE_PATH_RE.search(url) or _HANDLE_GENERIC_RE.search(url)
    return m.group(1) if m else None


def build_handle_url(handle: str) -> str:
    return f"{HANDLE_PREFIX}{handle}"


def needs_update(value: str) -> bool:
    handle = parse_handle(value)
    if not handle:
        return False
    expected = build_handle_url(handle)
    return value != expected


def build_patch(index: int, value: str) -> list:
    return [{"op": "replace", "path": f"/metadata/dc.identifier.uri/{index}/value", "value": value}]


class fixer:

    def __init__(self, dspace_be, dry_run: bool = False, skip_verify: bool = False):
        self._dspace_be = dspace_be
        self._dry_run = dry_run
        self._skip_verify = skip_verify
        self._stats = {"already_ok": 0, "updated": 0, "failed": 0, "no_handle": 0}
        self._invalid_handles = []  # (uuid, old_value, new_value)
        self._resolve_cache: Dict[str, bool] = {}

    @property
    def stats(self) -> dict:
        return self._stats

    @property
    def invalid_handles(self) -> list:
        return self._invalid_handles

    def _url_resolves(self, url: str) -> bool:
        if url in self._resolve_cache:
            return self._resolve_cache[url]
        try:
            r = requests.head(url, allow_redirects=True, timeout=10)
            if r.status_code == 200:
                self._resolve_cache[url] = True
                return True
            # Some servers reject HEAD; fall back to GET
            if r.status_code in (405, 501):
                with requests.get(url, allow_redirects=True, timeout=10, stream=True) as get_response:
                    result = get_response.status_code == 200
                self._resolve_cache[url] = result
                return result
            self._resolve_cache[url] = False
            return False
        except Exception as e:
            _logger.debug(f"HEAD [{url}] exception: {e}")
            self._resolve_cache[url] = False
            return False

    def fix_item(self, item: dict):
        uuid = item["uuid"]
        for idx, entry in enumerate(item.get("metadata", {}).get("dc.identifier.uri", [])):
            value = entry.get("value", "")
            if not needs_update(value):
                self._stats["already_ok"] += 1
                continue

            handle = parse_handle(value)
            if handle is None:
                _logger.warning(f"Item [{uuid}]: cannot extract handle from [{value}]")
                self._stats["no_handle"] += 1
                continue

            new_value = build_handle_url(handle)
            _logger.info(f"Item [{uuid}]: [{value}] -> [{new_value}]")

            if not self._skip_verify and not self._url_resolves(new_value):
                _logger.warning(f"Item [{uuid}]: [{new_value}] did not return HTTP 200 – skipping")
                self._invalid_handles.append((uuid, value, new_value))
                continue

            if not self._dry_run:
                patch = build_patch(idx, new_value)
                url = f"{self._dspace_be.endpoint}/core/items/{uuid}"
                r = self._dspace_be.client.api_patch(
                    url, self._dspace_be.client.PatchOperation.REPLACE, patch[0]["path"], new_value
                )
                if r is None or not r.ok:
                    _logger.error(f"Item [{uuid}]: PATCH failed [{value}] -> [{new_value}]")
                    self._stats["failed"] += 1
                    continue

            self._stats["updated"] += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"Normalize dc.identifier.uri values to {HANDLE_PREFIX} format"
    )
    parser.add_argument("--server", type=str, default=os.environ.get("DSPACE_ENDPOINT"))
    parser.add_argument("--user", type=str, default=os.environ.get("DSPACE_USER"))
    parser.add_argument("--password", type=str, default=os.environ.get("DSPACE_PASSWORD"))
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        default=False,
        help="Skip HTTP resolution check for new handle URLs (faster, for trusted environments)",
    )
    args = parser.parse_args()
    _log_args = {k: ("***" if k == "password" else v) for k, v in vars(args).items()}
    _logger.info(f"Arguments: {_log_args}")

    start = time.time()
    dspace_be = dspace.rest(args.server, args.user, args.password, True)

    _logger.info("Fetching and fixing items...")

    def iter_relevant_items():
        for page in dspace_be.iter_items():
            for item in page:
                if (
                    not item["withdrawn"]
                    and item["inArchive"]
                    and "dc.identifier.uri" in item.get("metadata", {})
                ):
                    yield item

    fix = fixer(dspace_be, dry_run=args.dry_run, skip_verify=args.skip_verify)
    total_with_uri = 0
    for item in tqdm.tqdm(iter_relevant_items(), desc="Fixing URIs", unit="item"):
        total_with_uri += 1
        fix.fix_item(item)

    _logger.info(40 * "=")
    s = fix.stats
    _logger.info(f"Total with_uri: {total_with_uri}  "
                 f"already_ok={s['already_ok']}  updated={s['updated']}  "
                 f"failed={s['failed']}  no_handle={s['no_handle']}  "
                 f"invalid_url={len(fix.invalid_handles)}")
    if fix.invalid_handles:
        _logger.warning("Handles that did not resolve (HTTP != 200):")
        for uuid, old_val, new_val in fix.invalid_handles:
            _logger.warning(f"  item [{uuid}]: [{old_val}] -> [{new_val}]")
    took = time.time() - start
    _logger.info(f"Total time: {took:.2f} s [{time.strftime('%H:%M:%S', time.gmtime(took))}]")
