import argparse
import logging
import os
import re
import sys
import time

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

# ORCID format: four groups of four digits separated by hyphens
_ORCID_RE = re.compile(r"\b(\d{4}-\d{4}-\d{4}-\d{3}[\dX])\b")


def parse_orcid_entry(value: str):
    """Parse one dc.identifier.orcid value into (name, orcid).
    Returns None if no ORCID pattern is found.
    """
    m = _ORCID_RE.search(value)
    if m is None:
        return None
    orcid = m.group(1)
    name = value[: m.start()].strip()
    return (name, orcid)


def build_orcid_map(metadata: dict) -> dict:
    """Return {author_name: orcid} from item metadata."""
    result = {}
    for entry in metadata.get("dc.identifier.orcid", []):
        parsed = parse_orcid_entry(entry.get("value", ""))
        if parsed:
            name, orcid = parsed
            result[name] = orcid
    return result


def match_author(author: str, orcid_map: dict):
    """Return ORCID for *author* if present in *orcid_map*, otherwise None."""
    return orcid_map.get(author)


def build_patch(index: int, orcid: str) -> list:
    return [{"op": "replace", "path": f"/metadata/dc.contributor.author/{index}/authority", "value": orcid}]


ORCID_URL = env["dspace"]["orcid_url"]


class updater:

    def __init__(self, dspace_be, dry_run: bool = False, skip_resolve: bool = False):
        self._dspace_be = dspace_be
        self._dry_run = dry_run
        self._skip_resolve = skip_resolve
        self._stats = {"already_ok": 0, "updated": 0, "failed": 0, "unresolvable": 0}
        self._invalid_orcids = []  # (uuid, author_name, orcid)
        self._orcid_cache: dict = {}  # orcid -> bool, cached resolution results

    @property
    def stats(self) -> dict:
        return self._stats

    @property
    def invalid_orcids(self) -> list:
        return self._invalid_orcids

    def _orcid_resolves(self, orcid: str) -> bool:
        if orcid in self._orcid_cache:
            return self._orcid_cache[orcid]
        url = ORCID_URL.format(orcid)
        try:
            r = requests.head(url, allow_redirects=True, timeout=10)
            if r.status_code == 200:
                result = True
            elif r.status_code in (405, 501):
                r = requests.get(url, allow_redirects=True, timeout=10, stream=True)
                result = r.status_code == 200
            else:
                result = False
        except Exception as e:
            _logger.debug(f"HEAD [{url}] exception: {e}")
            result = False
        self._orcid_cache[orcid] = result
        return result

    def update_item(self, item: dict):
        uuid = item["uuid"]
        metadata = item.get("metadata", {})
        orcid_map = build_orcid_map(metadata)
        if not orcid_map:
            return

        n_orcids = len(metadata.get("dc.identifier.orcid", []))
        n_authors = len(metadata.get("dc.contributor.author", []))
        _logger.debug(f"Item [{uuid}]: orcids={n_orcids}  authors={n_authors}")

        item_updated = 0
        authors = metadata.get("dc.contributor.author", [])
        for idx, entry in enumerate(authors):
            name = entry.get("value", "")
            orcid = match_author(name, orcid_map)
            if orcid is None:
                continue

            current_authority = entry.get("authority") or ""
            if current_authority == orcid:
                self._stats["already_ok"] += 1
                continue

            if not self._skip_resolve and not self._orcid_resolves(orcid):
                _logger.warning(f"Item [{uuid}]: author [{name}] ORCID [{orcid}] did not return HTTP 200 – skipping")
                self._invalid_orcids.append((uuid, name, orcid))
                self._stats["unresolvable"] += 1
                continue

            _logger.info(f"Item [{uuid}]: author [{name}] authority -> [{orcid}]")

            if not self._dry_run:
                patch = build_patch(idx, orcid)
                url = f"{self._dspace_be.endpoint}/core/items/{uuid}"
                r = self._dspace_be.client.api_patch(
                    url, self._dspace_be.client.PatchOperation.REPLACE, patch[0]["path"], orcid
                )
                if r is None or not r.ok:
                    _logger.error(f"Item [{uuid}]: PATCH failed for author [{name}]")
                    self._stats["failed"] += 1
                    continue

            item_updated += 1
            self._stats["updated"] += 1

        if item_updated:
            _logger.info(
                f"Item [{uuid}]: updated={item_updated}/{n_authors} authors  "
                f"orcids_available={len(orcid_map)}/{n_orcids}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set dc.contributor.author authority from dc.identifier.orcid values"
    )
    parser.add_argument("--server", type=str, default=os.environ.get("DSPACE_ENDPOINT"))
    parser.add_argument("--user", type=str, default=os.environ.get("DSPACE_USER"))
    parser.add_argument("--password", type=str, default=os.environ.get("DSPACE_PASSWORD"))
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--no-orcid-check", action="store_true", default=False,
                        help="Skip HTTP resolution check for each ORCID (faster, no rate-limit risk)")
    args = parser.parse_args()
    _log_args = {k: ("***" if k == "password" else v) for k, v in vars(args).items()}
    _logger.info(f"Arguments: {_log_args}")

    start = time.time()
    dspace_be = dspace.rest(args.server, args.user, args.password, True)

    upd = updater(dspace_be, dry_run=args.dry_run, skip_resolve=args.no_orcid_check)
    total_items = 0
    progress_desc = "Updating authors"
    with tqdm.tqdm(desc=progress_desc, unit="item", total=None) as progress:
        for page in dspace_be.iter_items():
            for item in page:
                if (
                    not item["withdrawn"]
                    and item["inArchive"]
                    and "dc.identifier.orcid" in item.get("metadata", {})
                    and "dc.contributor.author" in item.get("metadata", {})
                ):
                    upd.update_item(item)
                    total_items += 1
                    progress.update(1)

    _logger.info(40 * "=")
    s = upd.stats
    _logger.info(
        f"Total items: {total_items}  "
        f"already_ok={s['already_ok']}  updated={s['updated']}  "
        f"failed={s['failed']}  "
        f"unresolvable={s['unresolvable']}"
    )
    if upd.invalid_orcids:
        _logger.warning("ORCIDs that did not resolve (HTTP != 200):")
        for uuid, name, orcid in upd.invalid_orcids:
            _logger.warning(f"  item [{uuid}]: author [{name}] -> [{orcid}] ({ORCID_URL.format(orcid)})")
    took = time.time() - start
    _logger.info(f"Total time: {took:.2f} s [{time.strftime('%H:%M:%S', time.gmtime(took))}]")
