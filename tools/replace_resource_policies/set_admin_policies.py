"""Set READ policies on ORIGINAL bundles and bitstreams to Administrator only."""

import argparse
import logging
import os
import re
import sys
import time
from typing import Any, Optional

import requests

_this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_this_dir, "../../src"))

import settings  # noqa: E402
import project_settings  # noqa: E402
from utils import init_logging, update_settings  # noqa: E402

_logger = logging.getLogger()

# env settings, update with project_settings
env = update_settings(settings.env, project_settings.settings)
init_logging(_logger, env["log_file"])

DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_BACKOFF_SEC = 1.0
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# DSpace REST client
# ---------------------------------------------------------------------------

class DSpaceClient:
    """Thin wrapper around the DSpace 8 REST API."""

    def __init__(
        self,
        base_url: str,
        dry_run: bool = False,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        retry_count: int = DEFAULT_RETRY_COUNT,
        retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC,
    ):
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.timeout_sec = timeout_sec
        self.retry_count = retry_count
        self.retry_backoff_sec = retry_backoff_sec
        self.session = requests.Session()
        self.jwt: Optional[str] = None
        self.csrf_token: Optional[str] = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.jwt:
            headers["Authorization"] = f"Bearer {self.jwt}"
        if self.csrf_token:
            headers["X-XSRF-TOKEN"] = self.csrf_token
        return headers

    def _request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        data: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        allow_redirects: bool = True,
        retries: Optional[int] = None,
        retry_on_status: bool = True,
    ) -> requests.Response:
        """Send HTTP request with explicit timeout and bounded retry/backoff."""
        attempts = (self.retry_count if retries is None else retries) + 1
        last_exc: Optional[Exception] = None
        method_upper = method.upper()

        for attempt in range(1, attempts + 1):
            try:
                response = self.session.request(
                    method=method_upper,
                    url=url,
                    headers=headers,
                    params=params,
                    data=data,
                    json=json,
                    timeout=self.timeout_sec,
                    allow_redirects=allow_redirects,
                )
                if retry_on_status and response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < attempts:
                        delay = self.retry_backoff_sec * (2 ** (attempt - 1))
                        _logger.warning(
                            "%s %s returned %s; retrying in %.1fs (%d/%d)",
                            method_upper,
                            url,
                            response.status_code,
                            delay,
                            attempt,
                            attempts,
                        )
                        time.sleep(delay)
                        continue
                return response
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < attempts:
                    delay = self.retry_backoff_sec * (2 ** (attempt - 1))
                    _logger.warning(
                        "%s %s failed (%s); retrying in %.1fs (%d/%d)",
                        method_upper,
                        url,
                        exc,
                        delay,
                        attempt,
                        attempts,
                    )
                    time.sleep(delay)
                    continue
                break

        if last_exc is not None:
            raise RuntimeError(f"HTTP request failed after retries: {method_upper} {url}") from last_exc
        raise RuntimeError(f"HTTP request failed after retries: {method_upper} {url}")

    def _refresh_csrf(self) -> None:
        """Fetch a fresh CSRF token from /api/security/csrf."""
        response = self._request(
            "GET",
            f"{self.base_url}/api/security/csrf",
            headers={"Accept": "application/json"},
            retries=self.retry_count,
        )
        response.raise_for_status()
        # DSpace returns the token both in a response header and a cookie.
        token = response.headers.get("DSPACE-XSRF-TOKEN") or response.cookies.get(
            "DSPACE-XSRF-COOKIE"
        )
        if token:
            self.csrf_token = token
            _logger.debug("CSRF token refreshed")
        else:
            _logger.warning("Could not retrieve CSRF token from /api/security/csrf")

    def _get_all_pages(self, url: str, embedded_key: str) -> list[dict[str, Any]]:
        """
        Collect all items from a paginated HAL response.

        :param url:          Initial URL (may already have query params).
        :param embedded_key: Key inside '_embedded' that holds the list.
        :return:             Flat list of all items across all pages.
        """
        results: list[dict[str, Any]] = []
        # Force large page size to minimise round-trips
        sep = "&" if "?" in url else "?"
        next_url = f"{url}{sep}size=100&page=0"

        while next_url:
            response = self._request(
                "GET",
                next_url,
                headers=self._auth_headers(),
                retries=self.retry_count,
            )
            response.raise_for_status()
            data = response.json()

            embedded = data.get("_embedded", {})
            page_items = embedded.get(embedded_key, [])
            results.extend(page_items)

            page = data.get("page", {})
            total_pages = page.get("totalPages", 1)
            current_page = page.get("number", 0)

            if current_page + 1 < total_pages:
                # Build the URL for the next page
                base_url_no_page = re.sub(r"[?&]page=\d+", "", next_url)
                sep2 = "&" if "?" in base_url_no_page else "?"
                next_url = f"{base_url_no_page}{sep2}page={current_page + 1}"
            else:
                next_url = None

        return results

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def login(self, email: str, password: str) -> None:
        """Authenticate and store the JWT token."""
        self._refresh_csrf()

        response = self._request(
            "POST",
            f"{self.base_url}/api/authn/login",
            data={"user": email, "password": password},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-XSRF-TOKEN": self.csrf_token or "",
            },
            retry_on_status=False,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Login failed ({response.status_code}): {response.text[:200]}"
            )

        auth_header = response.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise RuntimeError("Login response did not contain a Bearer JWT token.")
        self.jwt = auth_header.split(" ", 1)[1]
        # Refresh CSRF after login (DSpace 8 may rotate it)
        self._refresh_csrf()
        _logger.info("Logged in successfully as %s", email)

    def logout(self) -> None:
        if not self.jwt:
            return
        self._request(
            "POST",
            f"{self.base_url}/api/authn/logout",
            headers=self._auth_headers(),
            retry_on_status=False,
        )
        _logger.info("Logged out.")
        self.jwt = None

    # ------------------------------------------------------------------
    # Group / Admin lookup
    # ------------------------------------------------------------------

    def find_administrator_group_uuid(self) -> str:
        """Return the UUID of the built-in 'Administrator' group."""
        groups = self._get_all_pages(
            f"{self.base_url}/api/eperson/groups/search/byMetadata?query=Administrator",
            "groups",
        )
        for group in groups:
            if group.get("name") == "Administrator":
                uuid = group["uuid"]
                _logger.info("Found Administrator group UUID: %s", uuid)
                return uuid
        raise RuntimeError(
            "Could not find the 'Administrator' group via the REST API. "
            "Ensure the account has admin privileges."
        )

    # ------------------------------------------------------------------
    # Handle resolution
    # ------------------------------------------------------------------

    def resolve_handle(self, handle: str) -> Optional[str]:
        """
        Resolve a handle string (e.g. '10283/1911') to an item UUID.
        First tries /api/pid/find (returns 302 redirect on success).
        Falls back to /api/discover/search/objects when pid/find returns 404.
        Returns None if the handle cannot be resolved.
        """
        response = self._request(
            "GET",
            f"{self.base_url}/api/pid/find",
            params={"id": handle},
            headers=self._auth_headers(),
            allow_redirects=False,
            retries=self.retry_count,
        )
        if response.status_code == 302:
            location = response.headers.get("Location", "")
            # Extract UUID from a URL like .../api/core/items/<uuid>
            match = re.search(r"/api/core/items/([0-9a-f-]{36})", location)
            if match:
                return match.group(1)
            _logger.warning("Unexpected redirect location for handle %s: %s", handle, location)
            return None

        if response.status_code not in (404, 200):
            _logger.warning(
                "Unexpected status %s when resolving handle %s via pid/find: %s",
                response.status_code, handle, response.text[:200],
            )

        # Fallback: search via discovery API
        _logger.debug(
            "pid/find returned %s for %s - trying discover search",
            response.status_code,
            handle,
        )
        discover_response = self._request(
            "GET",
            f"{self.base_url}/api/discover/search/objects",
            params={"query": f"handle:{handle}", "dsoType": "ITEM"},
            headers=self._auth_headers(),
            retries=self.retry_count,
        )
        if discover_response.status_code != 200:
            _logger.warning("Handle not found: %s", handle)
            return None
        data = discover_response.json()
        objects = (
            data.get("_embedded", {})
            .get("searchResult", {})
            .get("_embedded", {})
            .get("objects", [])
        )
        for obj in objects:
            embedded = obj.get("_embedded", {}).get("indexableObject", {})
            if embedded.get("handle") == handle:
                return embedded.get("uuid")
        _logger.warning("Handle not found in discover results: %s", handle)
        return None

    # ------------------------------------------------------------------
    # Item / Bundle / Bitstream
    # ------------------------------------------------------------------

    def get_original_bundle(self, item_uuid: str) -> Optional[dict]:
        """Return the ORIGINAL bundle object for the given item, or None."""
        bundles = self._get_all_pages(
            f"{self.base_url}/api/core/items/{item_uuid}/bundles",
            "bundles",
        )
        for bundle in bundles:
            if bundle.get("name") == "ORIGINAL":
                return bundle
        _logger.warning("No ORIGINAL bundle found for item %s", item_uuid)
        return None

    def get_bitstreams(self, bundle_uuid: str) -> list:
        """Return all bitstream objects from the given bundle."""
        return self._get_all_pages(
            f"{self.base_url}/api/core/bundles/{bundle_uuid}/bitstreams",
            "bitstreams",
        )

    # ------------------------------------------------------------------
    # Resource policies
    # ------------------------------------------------------------------

    def get_read_policies(self, resource_uuid: str) -> list:
        """Return all existing READ resource policies for a resource."""
        return self._get_all_pages(
            f"{self.base_url}/api/authz/resourcepolicies/search/resource"
            f"?uuid={resource_uuid}&action=READ",
            "resourcepolicies",
        )

    def delete_policy(self, policy_id: int) -> bool:
        """Delete a resource policy by its integer ID. Returns True on success."""
        if self.dry_run:
            _logger.info("[DRY-RUN] Would DELETE policy id=%s", policy_id)
            return True
        response = self._request(
            "DELETE",
            f"{self.base_url}/api/authz/resourcepolicies/{policy_id}",
            headers=self._auth_headers(),
            retries=self.retry_count,
        )
        if response.status_code == 204:
            _logger.debug("Deleted policy id=%s", policy_id)
            return True
        if response.status_code == 404:
            _logger.debug("Policy id=%s already gone (404)", policy_id)
            return True
        _logger.error(
            "Failed to delete policy id=%s: %s %s",
            policy_id,
            response.status_code,
            response.text[:200],
        )
        return False

    def create_read_policy(self, resource_uuid: str, admin_group_uuid: str) -> bool:
        """
        Create a READ resource policy on *resource_uuid* for the Administrator group.
        Returns True on success.
        """
        if self.dry_run:
            _logger.info(
                "[DRY-RUN] Would POST READ policy resource=%s group=%s",
                resource_uuid, admin_group_uuid,
            )
            return True

        response = self._request(
            "POST",
            f"{self.base_url}/api/authz/resourcepolicies",
            params={"resource": resource_uuid, "group": admin_group_uuid},
            json={"action": "READ", "type": "resourcepolicy"},
            headers={**self._auth_headers(), "Content-Type": "application/json"},
            retry_on_status=False,
        )
        if response.status_code in (200, 201):
            _logger.debug(
                "Created READ policy for resource=%s group=%s", resource_uuid, admin_group_uuid
            )
            return True
        _logger.error(
            "Failed to create READ policy for resource=%s: %s %s",
            resource_uuid,
            response.status_code,
            response.text[:200],
        )
        return False

    # ------------------------------------------------------------------
    # High-level operation
    # ------------------------------------------------------------------

    def restrict_to_admin(self, resource_uuid: str, admin_group_uuid: str, label: str) -> bool:
        """
        Replace all READ policies on *resource_uuid* with a single Administrator-only policy.
        *label* is used only for log messages (e.g. 'bundle', 'bitstream <name>').
        """
        existing_policies = self.get_read_policies(resource_uuid)
        _logger.info(
            "  %s [%s]: found %d existing READ polic(y/ies)",
            label, resource_uuid, len(existing_policies),
        )

        all_deleted = True
        for policy in existing_policies:
            policy_id = policy.get("id")
            if policy_id is not None:
                all_deleted = self.delete_policy(policy_id) and all_deleted

        created = self.create_read_policy(resource_uuid, admin_group_uuid)
        if all_deleted and created:
            _logger.info("  %s [%s]: READ policy set to Administrator only.", label, resource_uuid)
            return True
        _logger.error("  %s [%s]: failed to fully update READ policy.", label, resource_uuid)
        return False


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def parse_handles_csv(raw_handles: str) -> list[str]:
    """
    Parse comma-separated handles from CLI input.
    Accepts formats like:
        "12345/1001","12345/1002"
        12345/1001, 12345/1002
    """
    handles: list[str] = []
    # Split by comma, strip whitespace and surrounding quotes
    for part in raw_handles.split(","):
        handle = part.strip().strip('"').strip("'").strip()
        if handle:
            handles.append(handle)
    return handles


def collect_handles(raw_handles: Optional[str], repeated_handles: Optional[list[str]]) -> list[str]:
    """Merge handles from --handles and repeated --handle arguments."""
    merged: list[str] = []
    if raw_handles:
        merged.extend(parse_handles_csv(raw_handles))
    if repeated_handles:
        for handle in repeated_handles:
            val = handle.strip().strip('"').strip("'").strip()
            if val:
                merged.append(val)
    return merged


def process_handle(
    client: DSpaceClient,
    handle: str,
    admin_group_uuid: str,
    continue_on_bitstream_error: bool = False,
) -> dict[str, Any]:
    """Process one handle and restrict bundle + bitstreams to Administrator group."""
    result: dict[str, Any] = {
        "handle": handle,
        "success": False,
        "reason": None,
        "bitstreams_attempted": 0,
        "bitstreams_failed": 0,
    }

    _logger.info("=" * 60)
    _logger.info("Processing handle: %s", handle)

    item_uuid = client.resolve_handle(handle)
    if not item_uuid:
        _logger.warning("Skipping handle %s (could not resolve).", handle)
        result["reason"] = "unresolved_handle"
        return result

    _logger.info("  Resolved to item UUID: %s", item_uuid)

    original_bundle = client.get_original_bundle(item_uuid)
    if not original_bundle:
        _logger.warning("Skipping handle %s (no ORIGINAL bundle).", handle)
        result["reason"] = "missing_original_bundle"
        return result

    bundle_uuid = original_bundle["uuid"]
    _logger.info("  ORIGINAL bundle UUID: %s", bundle_uuid)

    bitstreams = client.get_bitstreams(bundle_uuid)
    _logger.info("  Found %d bitstream(s) in ORIGINAL bundle.", len(bitstreams))
    result["bitstreams_attempted"] = len(bitstreams)

    if not client.restrict_to_admin(bundle_uuid, admin_group_uuid, "ORIGINAL bundle"):
        result["reason"] = "bundle_policy_update_failed"
        return result

    failed_bitstreams = 0
    for bitstream in bitstreams:
        bitstream_uuid = bitstream["uuid"]
        bitstream_name = bitstream.get("name", bitstream_uuid)
        if not client.restrict_to_admin(
            bitstream_uuid,
            admin_group_uuid,
            f"bitstream '{bitstream_name}'",
        ):
            failed_bitstreams += 1
            result["bitstreams_failed"] = failed_bitstreams
            if continue_on_bitstream_error:
                _logger.warning(
                    "  bitstream '%s' [%s]: failed; continuing due to --continue-on-bitstream-error",
                    bitstream_name,
                    bitstream_uuid,
                )
                continue
            result["reason"] = "bitstream_policy_update_failed"
            _logger.info(
                "Handle %s summary: bitstreams attempted=%d, failed=%d",
                handle,
                result["bitstreams_attempted"],
                result["bitstreams_failed"],
            )
            return result

    if failed_bitstreams > 0:
        _logger.warning(
            "Handle %s finished with %d failed bitstream policy update(s).",
            handle,
            failed_bitstreams,
        )
        result["reason"] = "bitstream_policy_update_failed"
        _logger.info(
            "Handle %s summary: bitstreams attempted=%d, failed=%d",
            handle,
            result["bitstreams_attempted"],
            result["bitstreams_failed"],
        )
        return result

    _logger.info("Handle %s processed successfully.", handle)
    result["success"] = True
    _logger.info(
        "Handle %s summary: bitstreams attempted=%d, failed=%d",
        handle,
        result["bitstreams_attempted"],
        result["bitstreams_failed"],
    )
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set READ policies on ORIGINAL bundles/bitstreams to Administrator only."
    )
    handles_group = parser.add_mutually_exclusive_group(required=True)
    handles_group.add_argument(
        "--handles",
        help=(
            "Comma-separated item handles, e.g. \"12345/1001,12345/1002\". "
            "Quotes around each handle are optional."
        ),
    )
    handles_group.add_argument(
        "--handle",
        action="append",
        help="Single item handle. Repeat this argument to pass multiple handles.",
    )
    parser.add_argument(
        "--base-url",
        required=True,
        help="DSpace REST base URL, e.g. https://example-dspace.invalid/server",
    )
    parser.add_argument(
        "--email",
        required=True,
        help="Admin account e-mail used for login.",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="Admin password",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be done without actually modifying any policies.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=DEFAULT_TIMEOUT_SEC,
        help=f"HTTP request timeout in seconds (default: {DEFAULT_TIMEOUT_SEC}).",
    )
    parser.add_argument(
        "--retry-count",
        type=int,
        default=DEFAULT_RETRY_COUNT,
        help=f"Number of retries for retryable HTTP failures (default: {DEFAULT_RETRY_COUNT}).",
    )
    parser.add_argument(
        "--retry-backoff-sec",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_SEC,
        help=(
            "Base delay in seconds for exponential retry backoff "
            f"(default: {DEFAULT_RETRY_BACKOFF_SEC})."
        ),
    )
    parser.add_argument(
        "--continue-on-bitstream-error",
        action="store_true",
        help=(
            "Continue processing remaining bitstreams within a handle if one fails. "
            "Default behavior (without this flag) stops processing the handle on first failure."
        ),
    )
    args = parser.parse_args()

    if args.timeout_sec <= 0:
        _logger.error("--timeout-sec must be > 0")
        sys.exit(1)
    if args.retry_count < 0:
        _logger.error("--retry-count must be >= 0")
        sys.exit(1)
    if args.retry_backoff_sec < 0:
        _logger.error("--retry-backoff-sec must be >= 0")
        sys.exit(1)

    _logger.info(
        "Arguments: base_url=%s, email=%s, dry_run=%s, timeout_sec=%s, retry_count=%s, "
        "retry_backoff_sec=%s, continue_on_bitstream_error=%s, handles_mode=%s",
        args.base_url,
        args.email,
        args.dry_run,
        args.timeout_sec,
        args.retry_count,
        args.retry_backoff_sec,
        args.continue_on_bitstream_error,
        "--handles" if args.handles else "--handle",
    )

    handles = collect_handles(args.handles, args.handle)
    if not handles:
        _logger.error("No valid handles were provided via --handles or --handle")
        sys.exit(1)
    _logger.info("Loaded %d handle(s) from CLI arguments", len(handles))

    client = DSpaceClient(
        base_url=args.base_url,
        dry_run=args.dry_run,
        timeout_sec=args.timeout_sec,
        retry_count=args.retry_count,
        retry_backoff_sec=args.retry_backoff_sec,
    )

    try:
        client.login(args.email, args.password)
        admin_group_uuid = client.find_administrator_group_uuid()

        success_count = 0
        failure_count = 0
        total_bitstreams_attempted = 0
        total_bitstreams_failed = 0
        failure_reasons: dict[str, int] = {}

        for handle in handles:
            handle_result = process_handle(
                client,
                handle,
                admin_group_uuid,
                continue_on_bitstream_error=args.continue_on_bitstream_error,
            )

            total_bitstreams_attempted += int(handle_result["bitstreams_attempted"])
            total_bitstreams_failed += int(handle_result["bitstreams_failed"])

            if handle_result["success"]:
                success_count += 1
            else:
                failure_count += 1
                reason = str(handle_result["reason"] or "unknown")
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1

        _logger.info("=" * 60)
        _logger.info(
            "Done. %d handle(s) processed successfully, %d skipped/failed.",
            success_count, failure_count,
        )
        _logger.info(
            "Bitstream summary: attempted=%d, failed=%d",
            total_bitstreams_attempted,
            total_bitstreams_failed,
        )
        if failure_reasons:
            _logger.info("Aggregate failure report:")
            for reason, count in sorted(failure_reasons.items()):
                _logger.info("  %s: %d", reason, count)

    finally:
        client.logout()


if __name__ == "__main__":
    main()
