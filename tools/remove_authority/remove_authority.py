import argparse
import logging
import os
import re
import sys

# Set up directories for imports
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))

import utils

# load .env
dotenv_file = os.path.join(_this_dir, '../../src/', os.environ.get("ENVFILE", ".env"))
utils.load_env(dotenv_file)

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from utils import init_logging, update_settings  # noqa
logging.getLogger("dspace.client").setLevel(logging.WARNING)

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)

# DSpace Choices.CF_UNSET - confidence of a value that has no authority
CONFIDENCE_UNSET = -1
# authority of a value that comes from a relationship, the server owns those
VIRTUAL_PREFIX = "virtual::"
# `https://hdl.handle.net/` or `https://dspace.x.cz/handle/` in front of a handle
HANDLE_URL = re.compile(r"^https?://[^/]+/(handle/)?")


class UnexpectedState(Exception):
    """The item did not end up as asked for, so the whole run stops."""


def strip_handle_prefix(handle: str) -> str:
    """Turn `https://dspace.x.cz/handle/123456789/1` into `123456789/1`."""
    return HANDLE_URL.sub("", handle)


def load_handles(file_path: str) -> list:
    """Read handles, one per line. Empty lines and lines starting with # are ignored."""
    # utf-8-sig, a file saved by Notepad starts with a BOM that strip() keeps
    with open(file_path, "r", encoding="utf-8-sig") as fin:
        lines = [x.strip() for x in fin.readlines()]
    return [strip_handle_prefix(x) for x in lines if x and not x.startswith("#")]


def is_virtual(meta_val: dict) -> bool:
    """A `virtual::` authority comes from a relationship, the server owns it."""
    return (meta_val.get("authority") or "").startswith(VIRTUAL_PREFIX)


def is_removable(meta_val: dict) -> bool:
    return bool(meta_val.get("authority")) and not is_virtual(meta_val)


def with_authority(values: list) -> list:
    """(index, value) pairs of the values whose authority can be removed."""
    return [(i, x) for i, x in enumerate(values) if is_removable(x)]


def authority_patch_ops(field: str, to_clear: list) -> list:
    """Put every value back as it is, only without its authority."""
    return [{
        "op": "replace",
        "path": f"/metadata/{field}/{index}",
        "value": {
            "value": meta_val["value"],
            "language": meta_val.get("language"),
            "authority": None,
            "confidence": CONFIDENCE_UNSET,
        },
    } for index, meta_val in to_clear]


def verify(field: str, orig_values: list, updated_item: dict):
    """Raise UnexpectedState if the patches did not land where they were aimed."""
    new_values = updated_item.get("metadata", {}).get(field, [])
    if [x["value"] for x in new_values] != orig_values:
        raise UnexpectedState(f"[{field}] changed from {orig_values} "
                              f"to {[x['value'] for x in new_values]}")
    left = [x for x in new_values if is_removable(x)]
    if left:
        raise UnexpectedState(f"authority still present after the update {left}")


def process(dspace_be, handle: str, field: str, dry_run: bool) -> str:
    """Remove the authority from one item. Returns what happened to it."""
    item = dspace_be.fetch_item_by_handle(handle)
    if item is None:
        _logger.critical(f"[{handle}]: item not found")
        return "failed"

    values = item.get("metadata", {}).get(field, [])
    if not values:
        _logger.info(f"[{handle}]: no [{field}] on the item")
        return "no field"

    virtual = len([x for x in values if is_virtual(x)])
    if virtual:
        _logger.warning(f"[{handle}]: {virtual} relationship value(s) left alone")

    to_clear = with_authority(values)
    if not to_clear:
        _logger.info(f"[{handle}]: no authority in [{field}], nothing to do")
        return "skipped"

    if dry_run:
        _logger.info(
            f"[{handle}]: {len(to_clear)} authority value(s) of [{field}] - DRY-RUN")
        return "updated"

    # replacing by index does not reorder anything, the other values stay untouched
    orig_values = [x["value"] for x in values]
    updated = dspace_be.patch_metadata(item["uuid"], authority_patch_ops(field, to_clear))
    if updated is None:
        _logger.critical(f"[{handle}]: cannot patch [{field}], nothing was changed")
        return "failed"

    verify(field, orig_values, updated)
    _logger.info(f"[{handle}]: removed {len(to_clear)} authority value(s) of [{field}]")
    return "updated"


def run(dspace_be, handles: list, field: str, dry_run: bool):
    """Go through the handles. Returns the counts and the handles that failed."""
    counts = {"updated": 0, "skipped": 0, "no field": 0, "failed": 0}
    failed = []

    for handle in handles:
        try:
            result = process(dspace_be, handle, field, dry_run)
        except UnexpectedState as e:
            _logger.critical(f"[{handle}]: {e}")
            _logger.critical("Stopping, the remaining handles were not touched. See "
                             "'Before a production run' in the README of this tool.")
            counts["failed"] += 1
            failed.append(handle)
            break
        except Exception as e:
            # one broken connection must not throw away the rest of the run
            _logger.critical(f"[{handle}]: {e}", exc_info=True)
            result = "failed"

        counts[result] += 1
        if result == "failed":
            failed.append(handle)

    return counts, failed


def get_dspace_con(args):
    user = os.environ.get("DSPACE_USER", args.user)
    password = os.environ.get("DSPACE_PASSWORD", args.password)
    if "DSPACE_USER" in os.environ or "DSPACE_PASSWORD" in os.environ:
        _logger.info(f"Used environment variables: {user}")
    return dspace.rest(args.endpoint, user, password, True)


if __name__ == '__main__':
    init_logging(_logger, env["log_file"])

    parser = argparse.ArgumentParser(
        description="Remove the authority of a metadata field for the listed items")
    parser.add_argument("--handles",
                        type=str, required=True,
                        help="File with item handles, one per line, e.g. 123456789/1")
    parser.add_argument("--field",
                        type=str, default="dc.contributor.author",
                        help="Metadata field whose authority is removed.")
    parser.add_argument("--endpoint",
                        type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user",
                        type=str, default=env["backend"]["user"])
    parser.add_argument("--password",
                        type=str, default=env["backend"]["password"])
    parser.add_argument("--dry-run",
                        action='store_true', default=False)
    args = parser.parse_args()

    # output args from parse_args but without passwords
    args_dict = vars(args).copy()
    args_dict.pop("password", None)
    _logger.info(f"Arguments: {args_dict}")

    if not os.path.exists(args.handles):
        _logger.critical(f"File [{args.handles}] does not exist")
        sys.exit(1)

    handles = load_handles(args.handles)
    _logger.info(f"Loaded [{len(handles)}] handles from [{args.handles}]")

    dspace_be = get_dspace_con(args)
    counts, failed = run(dspace_be, handles, args.field, args.dry_run)

    _logger.info(40 * "=")
    if args.dry_run:
        _logger.info("DRY-RUN, nothing was changed")
    for name, cnt in counts.items():
        _logger.info(f"{name:10s}:{cnt:6d}")
    if handles and counts["no field"] == len(handles):
        _logger.critical(f"[{args.field}] was on none of the items, is the name right?")
    if failed:
        _logger.critical(f"Failed handles: {failed}")
        sys.exit(1)
