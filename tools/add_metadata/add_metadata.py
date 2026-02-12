import argparse
import logging
import time
import os
import json
import sys
from datetime import datetime
from collections import defaultdict

# Set up directories for imports
_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))
sys.path.insert(0, os.path.join(_this_dir, "../../src/pump"))

import utils

# load .env
dotenv_file = os.path.join(_this_dir, '../../src/', os.environ.get("ENVFILE", ".env"))
utils.load_env(dotenv_file)

import dspace  # noqa
import settings  # noqa
import project_settings  # noqa
from dspace_rest_client.models import Item
from utils import init_logging, update_settings  # noqa
logging.getLogger("dspace.client").setLevel(logging.WARNING)

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])


class iter_items_specific:
    def __init__(self, items, dspace_be):
        self.items = items
        self.dspace_be = dspace_be

    def __call__(self):
        for item_arr in self.items:
            uuid = item_arr[0]
            item_gen = self.dspace_be.iter_items(uuid=uuid)
            item_list = list(item_gen)
            yield item_list


def store_info(cache_file: str, d: dict, details: dict):
    new_d = {k: list(v) if isinstance(v, set) else v for k, v in d.items()}
    data = {
        "data": new_d,
        "details": details,
    }
    with open(cache_file, "w") as fout:
        json.dump(data, fout, indent=2, sort_keys=True)
    _logger.info(f"Stored info to [{cache_file}]")


class date:
    invalid = defaultdict(int)
    invalid_but_converted = defaultdict(int)

    def __init__(self, d: str):
        self._input = d
        self._d = d

    @property
    def input(self) -> str:
        return self._input

    @property
    def value(self) -> str:
        return self._d

    @staticmethod
    def is_year_only(date_str: str) -> bool:
        """Check if the string represents a year-only format (YYYY)."""
        if len(date_str) != 4 or not date_str.isdigit():
            return False
        try:
            datetime.strptime(date_str, '%Y')
            return True
        except ValueError:
            return False

    def is_valid_hybrid(self):
        """Check if date is valid in YYYY-MM-DD, YYYY-MM, or YYYY format (all kept as-is)."""
        # Check YYYY-MM-DD format
        try:
            datetime.strptime(self._d, '%Y-%m-%d')
            return True
        except ValueError:
            pass
        
        # Check YYYY-MM format (partial date)
        try:
            datetime.strptime(self._d, '%Y-%m')
            return True
        except ValueError:
            pass
        
        # Check YYYY format (year only)
        if date.is_year_only(self._d):
            return True
        
        date.invalid[self._d] += 1
        if date.invalid[self._d] == 1:
            _logger.debug(f"[{self._d}] is not valid date format (expected YYYY-MM-DD, YYYY-MM, or YYYY)")
        return False

    def parse_hybrid(self) -> bool:
        """Convert date with hybrid rules:
        - Keep YYYY format as-is (year only)
        - Keep YYYY-MM format as-is (partial date, but normalize separators)
        - Convert full dates to YYYY-MM-DD format
        """
        if len(self._d) < 1:
            return False

        # Check if it's already year-only format (YYYY) - keep as-is
        if date.is_year_only(self._d):
            return True

        # Try full date formats (with day, month, and year)
        full_date_formats = ['%Y/%m/%d', '%d/%m/%Y', '%Y.%m.%d', '%d.%m.%Y', 
                             '%Y-%m-%d', '%d-%m-%Y', '%d. %m. %Y']
        for fmt in full_date_formats:
            try:
                datetime_obj = datetime.strptime(self._d, fmt)
                # Normalize to 'YYYY-MM-DD'
                self._d = datetime_obj.strftime('%Y-%m-%d')
                return True
            except ValueError:
                continue
        
        # Try partial date formats (year-month) - normalize to YYYY-MM
        partial_formats = ['%Y-%m', '%m-%Y', '%Y/%m', '%m/%Y', '%Y.%m', '%m.%Y']
        for fmt in partial_formats:
            try:
                datetime_obj = datetime.strptime(self._d, fmt)
                # Normalize to 'YYYY-MM' (keep as partial date)
                self._d = datetime_obj.strftime('%Y-%m')
                return True
            except ValueError:
                continue
        
        _logger.warning(f"Error converting [{self._d}] to date.")
        return False


def update_item(item_d: dict):
    item = Item(item_d)
    if dspace_be.client.update_item(item):
        return True
    # Try to authenticate
    _logger.info("Reauthorization during item updating")
    if dspace_be.client.authenticate():
        dso = dspace_be.client.update_item(item)
        return dso is not None
    return False


class updater:

    ret_already_ok = 0
    ret_failed = 1
    ret_updated = 2
    ret_created = 3
    ret_invalid_meta = 4
    ret_empty_meta = 4

    def __init__(self, dspace_be, from_mtd_fields: list, to_mtd_field: list, dry_run: bool = False):
        self._dspace_be = dspace_be
        self._from_mtd_fields = from_mtd_fields
        self._to_mtd_field = to_mtd_field
        self._dry_run = dry_run
        self._info = {
            "valid": [],
            "valid_year_only": [],
            "multiple": set(),
            "invalid_date": [],
            "invalid_date_all": set(),
            "anomalies": [],
            "updated": [],
            "error_updating": [],
            "error_creating": [],
            "created": [],
            "not_created": [],
        }

    @property
    def cannot_parse(self):
        return self._info["invalid_date_all"]

    @property
    def info(self):
        return self._info

    def find_correct_metadata(self, item: dict):
        uuid = item['uuid']
        item_mtd = item["metadata"]

        for from_mtd in self._from_mtd_fields:
            meta_key = item_mtd.get(from_mtd, None)
            if meta_key is None:
                continue
            id_str = f"Item [{uuid}]: [{from_mtd}]"
            if len(meta_key) != 1:
                _logger.warning(f"{id_str}: more than one value {meta_key}")

            # If there is more than one value, get only the first one
            meta_val = date(meta_key[0]["value"])
            # Convert date if necessary
            if not meta_val.is_valid_hybrid():
                if not meta_val.parse_hybrid():
                    self._info["invalid_date_all"].add(meta_val.input)
                    continue
            return meta_val, id_str

        return None, None

    def _perform_update(self, item: dict, date_val: date, uuid: str, id_str: str) -> int:
        """Common logic for updating item metadata in database."""
        item_mtd = item["metadata"]
        
        # Log conversion
        date.invalid_but_converted[date_val.input] += 1
        if date.invalid_but_converted[date_val.input] == 1:
            _logger.info(f"{id_str}: invalid date [{date_val.input}] converted to [{date_val.value}]")

        # Update the item metadata with the converted date
        item_mtd[self._to_mtd_field][0]["value"] = date_val.value
        item["metadata"] = item_mtd

        # Update the item in the database
        updated_ok = self._dry_run or update_item(item)
        if not updated_ok:
            _logger.error(f"{id_str}: error updating item")
            self._info["error_updating"].append((uuid, date_val.input))
            return updater.ret_failed

        self._info["updated"].append((uuid, date_val.input))
        return updater.ret_updated

    def update_existing_metadata(self, item: dict, date_str: str, force: bool = False) -> int:
        """Update existing metadata with hybrid rules:
        - No null/empty handling (will crash on None)
        - YYYY formats kept as-is
        - Partial dates (YYYY-MM) kept as-is (normalized)
        - Invalid dates logged as ANOMALY
        """
        uuid = item['uuid']
        id_str = f"Item [{uuid}]: [{self._to_mtd_field}]"
        
        # No null/empty handling - let it crash if needed
        date_val = date(date_str)
        if not force:
            if date_val.is_valid_hybrid():
                # Check if it's year-only or partial format
                if date.is_year_only(date_str):
                    self._info["valid_year_only"].append((uuid, date_val.input))
                    _logger.info(f"{id_str}: year-only format [{date_str}] - keeping as-is")
                elif len(date_str) == 7 and date_str[4] == '-':  # YYYY-MM format
                    self._info["valid"].append((uuid, date_val.input))
                    _logger.info(f"{id_str}: partial date format [{date_str}] - keeping as-is")
                else:
                    self._info["valid"].append((uuid, date_val.input))
                return updater.ret_already_ok

            parsed_ok = date_val.parse_hybrid()
            if parsed_ok is False:
                _logger.error(f"{id_str}: cannot convert [{date_val.input}] to date - ANOMALY")
                self._info["invalid_date"].append((uuid, date_val.input))
                self._info["anomalies"].append((uuid, date_val.input, "Cannot parse date format"))
                return updater.ret_invalid_meta

        return self._perform_update(item, date_val, uuid, id_str)

    def add_new_metadata(self, item) -> int:
        uuid = item['uuid']

        meta_val, id_str = self.find_correct_metadata(item)
        if meta_val is not None:
            _logger.info(f"{id_str}: created...")
            added = (self._dry_run or
                     self._dspace_be.client.add_metadata(Item(item), self._to_mtd_field, meta_val.value))
            if not added:
                _logger.critical(f"{id_str}: Error creating metadata")
                self._info["error_creating"].append((uuid, meta_val.input))
                return updater.ret_failed

            self._info["created"].append((uuid, meta_val.input))
            return updater.ret_created

        self._info["not_created"].append((uuid, None))
        return updater.ret_empty_meta

    def update(self, item: dict, force: bool = False) -> int:
        """Create missing metadata for items based on provided fields."""
        item_mtd = item["metadata"]
        uuid = item['uuid']

        # Check if the target metadata field exists and is not empty
        date_meta = item_mtd.get(self._to_mtd_field, None)
        if date_meta is not None:
            val = date_meta[0]["value"]
            if len(date_meta) != 1:
                _logger.critical(f"{uuid}: other than one value {date_meta}")
                self._info["multiple"].add(uuid)
                if not self._dry_run:
                    val = ''
                    for i in range(len(date_meta)):
                        if len(val) == 0:
                            date_val = date(date_meta[i]["value"])
                            if date_val.is_valid_hybrid() or date_val.parse_hybrid():
                                val = date_val.value
                                continue
                        if val == '' and i == len(date_meta) - 1:
                            val = date_meta[i]["value"]
                            continue
                        dspace_be.client.remove_metadata(
                            Item(item), self._to_mtd_field, i)
                    # Reload item and metadata
                    item = dspace_be._fetch(f'core/items/{uuid}', dspace_be.get, None)

            # force change of metadata
            if force:
                meta_val, id_str = self.find_correct_metadata(item)
                if meta_val is not None:
                    if meta_val.value == val:
                        return updater.ret_already_ok
                    _logger.info(
                        f"{uuid}: forced change of metadata: {val} -> {meta_val.value}")
                    val = meta_val.value
                else:
                    _logger.info(
                        f"Forced metadata change but no value found for [{uuid}]")
                    return updater.ret_empty_meta

            # Always use standard validation/parsing (no relaxed mode)
            return self.update_existing_metadata(item, val, force=force)
        else:
            return self.add_new_metadata(item)


class additional_stats:

    def __init__(self):
        self._titles = defaultdict(int)
        self._doubles = defaultdict(list)

    def update(self, item: dict):
        uuid = item['uuid']
        dc_titles = item['metadata'].get('dc.title', [])
        if len(dc_titles) > 0:
            self._titles[dc_titles[0]['value']] += 1
        key = 'dc.date.issued'
        if len(item['metadata'].get(key, [])) > 1:
            self._doubles[key].append(uuid)

    def print_info(self, show_limit=100):
        duplicates = {k: v for k, v in self._titles.items() if v > 1}
        _logger.info(
            f"Duplicates {len(duplicates)} ({sum(duplicates.values())})  (showing first {show_limit}):")
        for i, (k, v) in enumerate(duplicates.items()):
            if i >= show_limit:
                break
            _logger.info(f"Title [{k}] : {v}")
        if len(self._doubles) > 0:
            _logger.info("Multiple values when expecting at most 1:")
            for k, v in self._doubles.items():
                _logger.info(f"{k}: {v}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Add metadata for DSpace items")
    parser.add_argument("--to_mtd_field",
                        type=str, required=False, help="Metadata field to be created or updated (required unless --fix-date-format is used).")
    parser.add_argument("--from_mtd_field",
                        type=str, nargs='+', required=False,
                        help="Metadata field(s) from which value(s) can be used.")
    parser.add_argument("--fix-date-format", action='store_true', default=False,
                        help="Fix date format in dc.date.issued field (no other parameters needed)")
    parser.add_argument("--endpoint", type=str, default=env["backend"]["endpoint"])
    parser.add_argument("--user", type=str, default=env["backend"]["user"])
    parser.add_argument("--password", type=str, default=env["backend"]["password"])
    parser.add_argument("--dry-run", action='store_true', default=False)
    parser.add_argument("--result-every-N", type=int, default=10000)
    parser.add_argument("--only", type=str, default=None)
    args = parser.parse_args()
    
    # Handle fix-date-format mode
    if args.fix_date_format:
        args.to_mtd_field = "dc.date.issued"
        args.from_mtd_field = ["dc.date.issued"]
        _logger.info("Fix date format mode enabled: correcting dc.date.issued")
    
    # Validate required arguments for non-fix-date-format mode
    if not args.fix_date_format:
        if args.to_mtd_field is None or args.from_mtd_field is None:
            parser.error("--to_mtd_field and --from_mtd_field are required unless --fix-date-format is used")
    
    # output args from parse_args but without passwords
    args_dict = vars(args).copy()
    args_dict.pop("password", None)
    _logger.info(f"Arguments: {args_dict}")

    output_info = os.path.join(_this_dir, "__results.json")
    _logger.info(f"Output info file: {output_info}")

    start = time.time()
    user = os.environ.get("DSPACE_USER", args.user)
    password = os.environ.get("DSPACE_PASSWORD", args.password)
    endpoint = args.endpoint.rstrip("/")
    if "DSPACE_USER" in os.environ or "DSPACE_PASSWORD" in os.environ:
        _logger.info(f"Used environment variables: {user}")

    # Initialize DSpace backend
    dspace_be = dspace.rest(endpoint, user, password, True)

    upd = updater(dspace_be, args.from_mtd_field, args.to_mtd_field, 
                  dry_run=args.dry_run)

    stats = additional_stats()

    fe_url = endpoint.split("/server")[0]

    cur_i = 0

    force = False
    if args.only is None:
        iter_items = dspace_be.iter_items
    else:
        if not os.path.exists(args.only):
            _logger.error(f"File [{args.only}] does not exist")
            sys.exit(1)
        try:
            with open(args.only, "r") as fin:
                items = json.load(fin)
        except:
            with open(args.only, "r", encoding="utf-8") as fin:
                items = [(x.strip(), None) for x in fin.read().splitlines()
                         if len(x.strip()) > 0]
        _logger.info(f"Loaded [{len(items)}] items from [{args.only}]")
        iter_items = iter_items_specific(items, dspace_be)
        force = True

    # Process items
    len_all_items = 0
    len_used_items = 0
    verify_failed = []
    i = 0

    for items in iter_items():
        cur_i += len(items)
        len_all_items += len(items)
        items = [item for item in items if not item['withdrawn'] and item['inArchive']]
        len_used_items += len(items)
        for item in items:
            i += 1
            uuid = item['uuid']
            item_url = f"{fe_url}/items/{uuid}"
            msg_header = f"{i:5d}: Item [ {item_url} ]"
            orig_values = [x['value']
                           for x in item.get("metadata", {}).get(args.to_mtd_field, [])]
            stats.update(item)
            ret_updated = upd.update(item, force=force)

            if ret_updated == updater.ret_already_ok:
                _logger.info(f"{msg_header}: already correct")
                continue

            # serious
            if ret_updated == updater.ret_failed:
                _logger.critical(f"{msg_header} failed to update metadata")
                continue

            if ret_updated == updater.ret_invalid_meta:
                _logger.warning(
                    f"{msg_header} does not have correct metadata [{orig_values}]")
                continue
            if ret_updated == updater.ret_empty_meta:
                _logger.warning(
                    f"{msg_header} does not have specified metadata [{args.from_mtd_field}]")
                continue

            if args.dry_run:
                _logger.info(f"{msg_header} updated - {orig_values} -> DRY-RUN")  # noqa
                continue

            # something changed, double verify
            if ret_updated in (updater.ret_created, updater.ret_updated):
                new_item = dspace_be._fetch(f'core/items/{uuid}', dspace_be.get, key=None)
                new_values = [x['value'] for x in new_item.get("metadata", {}).get(args.to_mtd_field, [])]  # noqa
                if len(new_values) == 0 or orig_values == new_values:
                    _logger.error(f"{msg_header} does not have correct metadata [{orig_values}]->[{new_values}] after create/update")  # noqa
                    verify_failed.append((uuid, item_url, orig_values))
                else:
                    _logger.info(f"{msg_header} updated - {orig_values} -> {new_values}")  # noqa
            else:
                _logger.error(
                    f"{msg_header} returned unexpected value [{ret_updated}]")

        # store intermediate outputs
        if cur_i > args.result_every_N:
            store_info(output_info, upd.info, {"args_dict": args_dict})
            cur_i = 0

    store_info(output_info, upd.info, {"args_dict": args_dict})

    _logger.info(40 * "=")
    _logger.info("Item info:")
    limit = 50
    for k, v in upd.info.items():
        _logger.info(f"{k:20s}:{len(v):6d}: first {limit} items .. {list(v)[:limit]}...")

    _logger.info(40 * "=")
    _logger.info("Anomalies found:")
    if len(upd.info["anomalies"]) > 0:
        _logger.warning(f"Total anomalies: {len(upd.info['anomalies'])}")
        for uuid, value, reason in upd.info["anomalies"][:100]:  # Show first 100
            _logger.warning(f"  Item [{uuid}]: value=[{value}] - {reason}")
    else:
        _logger.info("No anomalies found")

    _logger.info(40 * "=")
    _logger.info("Date info")
    msgs = "\n\t".join(upd.cannot_parse)
    _logger.info(f"Cannot parse [{len(msgs)}]:\n\t{msgs}")
    inv_arr = [(v, f"[{k:15s}]: {v:4d}") for k, v in date.invalid.items()]
    inv_arr.sort(key=lambda x: x[0], reverse=True)
    msgs = "\n\t".join([x[1] for x in inv_arr])
    _logger.info(f"Date invalid [{len(msgs)}]:\n\t{msgs}")

    _logger.info(40 * "=")
    stats.print_info()

    _logger.info(40 * "=")
    _logger.info("Update statistics:")
    for k, v in upd.info.items():
        _logger.info(f"{k:25s}: {len(v):6d}")
    took = time.time() - start

    _logger.info(40 * "=")
    _logger.info("Counts:")
    _logger.info(f"Total items: {len_all_items}")
    _logger.info(f"Used items: {len_used_items}")
    # sets are not counted
    _logger.info(
        f"Sum of updates: {sum(len(x) for x in upd.info.values() if isinstance(x, list))}")

    _logger.info(40 * "=")
    _logger.info(
        f"Total time: {took:.2f} s [{time.strftime('%H:%M:%S', time.gmtime(took))}]")
