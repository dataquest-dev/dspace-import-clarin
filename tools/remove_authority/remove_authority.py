import argparse
import logging
import os
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
init_logging(_logger, env["log_file"])

# DSpace Choices.CF_UNSET - confidence of a value that has no authority
CONFIDENCE_UNSET = -1


def strip_handle_prefix(handle: str) -> str:
    """Turn `https://dspace.x.cz/handle/123456789/1` into `123456789/1`."""
    for prefix in env["dspace"]["handle_prefix"]:
        if handle.startswith(prefix):
            return handle[len(prefix):]
    if "/handle/" in handle:
        return handle.split("/handle/", 1)[1]
    return handle


def load_handles(file_path: str) -> list:
    """Read handles, one per line. Empty lines and lines starting with # are ignored."""
    with open(file_path, "r", encoding="utf-8") as fin:
        lines = [x.strip() for x in fin.readlines()]
    return [strip_handle_prefix(x) for x in lines if x and not x.startswith("#")]


def with_authority(item: dict, field: str) -> list:
    """(index, value) pairs of the `field` values that still have an authority."""
    values = item.get("metadata", {}).get(field, [])
    return [(i, x) for i, x in enumerate(values) if x.get("authority")]


def patch_authority_away(dspace_be, item_url: str, field: str,
                         index: int, meta_val: dict):
    """Replace one metadata value by the same value without its authority."""
    new_val = {
        "value": meta_val["value"],
        "language": meta_val.get("language"),
        "authority": None,
        "confidence": CONFIDENCE_UNSET,
    }
    path = f"/metadata/{field}/{index}"
    r = dspace_be.client.api_patch(item_url, "replace", path, new_val)
    if r is not None and r.status_code == 401:
        _logger.info("Reauthorization during item updating")
        dspace_be.client.authenticate()
        r = dspace_be.client.api_patch(item_url, "replace", path, new_val)
    return r


def process(dspace_be, handle: str, field: str, dry_run: bool) -> str:
    """Remove the authority from one item. Returns `updated`, `skipped` or `failed`."""
    item = dspace_be.fetch_item_by_handle(handle)
    if item is None:
        _logger.critical(f"[{handle}]: item not found")
        return "failed"

    to_clear = with_authority(item, field)
    if not to_clear:
        _logger.info(f"[{handle}]: no authority in [{field}], nothing to do")
        return "skipped"

    if dry_run:
        _logger.info(
            f"[{handle}]: {len(to_clear)} authority value(s) of [{field}] - DRY-RUN")
        return "updated"

    # patching by index does not reorder anything, the other values stay untouched
    item_url = f"{dspace_be.endpoint}/core/items/{item['uuid']}"
    orig_values = [x["value"] for x in item["metadata"][field]]
    response = None
    for index, meta_val in to_clear:
        response = patch_authority_away(dspace_be, item_url, field, index, meta_val)
        if response is None or response.status_code != 200:
            code = response.status_code if response is not None else None
            _logger.critical(
                f"[{handle}]: cannot patch [{field}][{index}], status [{code}]")
            return "failed"

    new_values = response.json().get("metadata", {}).get(field, [])
    if [x["value"] for x in new_values] != orig_values:
        _logger.critical(f"[{handle}]: [{field}] changed from {orig_values} to "
                         f"{[x['value'] for x in new_values]}, check the item")
        return "failed"

    left = [x for x in new_values if x.get("authority")]
    if left:
        _logger.critical(f"[{handle}]: authority still present after the update {left}")
        return "failed"

    _logger.info(f"[{handle}]: removed {len(to_clear)} authority value(s) of [{field}]")
    return "updated"


def get_dspace_con(args):
    user = os.environ.get("DSPACE_USER", args.user)
    password = os.environ.get("DSPACE_PASSWORD", args.password)
    if "DSPACE_USER" in os.environ or "DSPACE_PASSWORD" in os.environ:
        _logger.info(f"Used environment variables: {user}")
    return dspace.rest(args.endpoint.rstrip("/"), user, password, True)


if __name__ == '__main__':
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

    handles = load_handles(args.handles)
    _logger.info(f"Loaded [{len(handles)}] handles from [{args.handles}]")

    dspace_be = get_dspace_con(args)
    counts = {"updated": 0, "skipped": 0, "failed": 0}
    failed = []

    for item_handle in handles:
        try:
            result = process(dspace_be, item_handle, args.field, args.dry_run)
        except Exception as e:
            # one broken connection must not throw away the rest of the run
            _logger.critical(f"[{item_handle}]: {e}", exc_info=True)
            result = "failed"
        counts[result] += 1
        if result == "failed":
            failed.append(item_handle)

    _logger.info(40 * "=")
    if args.dry_run:
        _logger.info("DRY-RUN, nothing was changed")
    for name, cnt in counts.items():
        _logger.info(f"{name:10s}:{cnt:6d}")
    if failed:
        _logger.critical(f"Failed handles: {failed}")
        sys.exit(1)
