"""Replace resource policies on items' ORIGINAL bundle (and bitstreams) by handle.

Standalone tool: resolves DSpace item handles to UUIDs, finds each item's
ORIGINAL bundle and its bitstreams, then replaces all existing resource
policies for a given action (default READ) with a single policy granting that
action to a configurable group (default 'Administrator').

No dependency on the dspace-import-clarin repository. Base URL and credentials
are passed at runtime via CLI arguments / environment variables only.

Authentication follows the DSpace 7/8 flow (same as dataquest-dev PR #264):
    1. GET  /api/security/csrf            -> obtain XSRF token
    2. POST /api/authn/login              -> read 'Authorization: Bearer <jwt>'
    3. send 'Authorization: Bearer <jwt>' + 'X-XSRF-TOKEN' on every request
"""

from __future__ import annotations

import argparse
import getpass
import logging
import os
import re
import sys
import time
from typing import Any, Optional

import requests

_logger = logging.getLogger("set_original_policies")

DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_BACKOFF_SEC = 1.0
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

# DSpace authorization actions accepted by the REST API.
KNOWN_ACTIONS = ["READ", "WRITE", "ADD", "REMOVE", "DELETE", "ADMIN"]

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                     r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def init_logging(logger: logging.Logger, log_file: Optional[str] = None,
                 level: int = logging.INFO) -> None:
    """Configure a stdout (and optional file) logger. Standalone, no repo deps."""
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)


# ---------------------------------------------------------------------------
# DSpace REST client
# ---------------------------------------------------------------------------

class DSpaceClient:
    """Thin wrapper around the DSpace 7/8 REST API with Bearer authentication."""

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
        # True when the JWT was supplied externally (we then must NOT log it out).
        self._external_token = False

    # ------------------------------------------------------------------
    # Low-level helpers
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
        """Send an HTTP request with explicit timeout and bounded retry/backoff."""
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
                            method_upper, url, response.status_code,
                            delay, attempt, attempts,
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
                        method_upper, url, exc, delay, attempt, attempts,
                    )
                    time.sleep(delay)
                    continue
                break

        if last_exc is not None:
            raise RuntimeError(
                f"HTTP request failed after retries: {method_upper} {url}") from last_exc
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
        """Collect all entries from a paginated HAL response across pages."""
        results: list[dict[str, Any]] = []
        sep = "&" if "?" in url else "?"
        next_url = f"{url}{sep}size=100&page=0"

        while next_url:
            response = self._request(
                "GET", next_url, headers=self._auth_headers(), retries=self.retry_count,
            )
            response.raise_for_status()
            data = response.json()

            embedded = data.get("_embedded", {})
            results.extend(embedded.get(embedded_key, []))

            page = data.get("page", {})
            total_pages = page.get("totalPages", 1)
            current_page = page.get("number", 0)

            if current_page + 1 < total_pages:
                base_url_no_page = re.sub(r"[?&]page=\d+", "", next_url)
                sep2 = "&" if "?" in base_url_no_page else "?"
                next_url = f"{base_url_no_page}{sep2}page={current_page + 1}"
            else:
                next_url = None

        return results

    # ------------------------------------------------------------------
    # Authentication (Bearer JWT, same flow as PR #264)
    # ------------------------------------------------------------------

    def login(self, email: str, password: str) -> None:
        """Authenticate and store the Bearer JWT token."""
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
        # DSpace 8 may rotate the CSRF token after login.
        self._refresh_csrf()
        _logger.info("Logged in successfully as %s", email)

    def use_bearer_token(self, token: str) -> None:
        """Authenticate with a pre-obtained Bearer JWT, skipping credential login.

        Still fetches a fresh CSRF token, which DSpace requires for the
        mutating (DELETE/POST) requests this tool makes.
        """
        self.jwt = token
        self._external_token = True
        self._refresh_csrf()
        _logger.info("Using provided Bearer token (credential login skipped).")

    def logout(self) -> None:
        if not self.jwt:
            return
        if self._external_token:
            # Don't invalidate a token the caller supplied and may reuse.
            _logger.info("Skipping logout (externally provided Bearer token).")
            self.jwt = None
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
    # Group resolution (by UUID or by name)
    # ------------------------------------------------------------------

    def resolve_group_uuid(self, group: str) -> str:
        """Resolve a group given as a UUID or a group name to its UUID."""
        if UUID_RE.match(group):
            _logger.info("Using group UUID directly: %s", group)
            return group

        groups = self._get_all_pages(
            f"{self.base_url}/api/eperson/groups/search/byMetadata?query={group}",
            "groups",
        )
        for grp in groups:
            if grp.get("name") == group:
                uuid = grp["uuid"]
                _logger.info("Resolved group '%s' to UUID: %s", group, uuid)
                return uuid
        raise RuntimeError(
            f"Could not find a group named '{group}' via the REST API. "
            "Pass an exact group name or a group UUID, and ensure the account "
            "has privileges to read groups."
        )

    # ------------------------------------------------------------------
    # Handle resolution
    # ------------------------------------------------------------------

    def resolve_handle(self, handle: str) -> Optional[str]:
        """Resolve a handle (e.g. '10283/1911') to an item UUID, or None.

        Tries /api/pid/find (302 redirect on success), falling back to the
        discovery search API.
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
            match = re.search(r"/api/core/items/([0-9a-f-]{36})", location)
            if match:
                return match.group(1)
            _logger.warning("Unexpected redirect location for handle %s: %s",
                            handle, location)
            return None

        if response.status_code not in (404, 200):
            _logger.warning(
                "Unexpected status %s when resolving handle %s via pid/find: %s",
                response.status_code, handle, response.text[:200],
            )

        _logger.debug("pid/find returned %s for %s - trying discover search",
                      response.status_code, handle)
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
            f"{self.base_url}/api/core/items/{item_uuid}/bundles", "bundles",
        )
        for bundle in bundles:
            if bundle.get("name") == "ORIGINAL":
                return bundle
        _logger.warning("No ORIGINAL bundle found for item %s", item_uuid)
        return None

    def get_bitstreams(self, bundle_uuid: str) -> list:
        """Return all bitstream objects from the given bundle."""
        return self._get_all_pages(
            f"{self.base_url}/api/core/bundles/{bundle_uuid}/bitstreams", "bitstreams",
        )

    # ------------------------------------------------------------------
    # Resource policies
    # ------------------------------------------------------------------

    def get_policies(self, resource_uuid: str, action: str) -> list:
        """Return existing resource policies for a resource filtered by action."""
        return self._get_all_pages(
            f"{self.base_url}/api/authz/resourcepolicies/search/resource"
            f"?uuid={resource_uuid}&action={action}",
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
        _logger.error("Failed to delete policy id=%s: %s %s",
                      policy_id, response.status_code, response.text[:200])
        return False

    def create_policy(self, resource_uuid: str, group_uuid: str, action: str) -> bool:
        """Create a resource policy granting *action* to *group_uuid* on *resource_uuid*."""
        if self.dry_run:
            _logger.info("[DRY-RUN] Would POST %s policy resource=%s group=%s",
                         action, resource_uuid, group_uuid)
            return True

        response = self._request(
            "POST",
            f"{self.base_url}/api/authz/resourcepolicies",
            params={"resource": resource_uuid, "group": group_uuid},
            json={"action": action, "type": "resourcepolicy"},
            headers={**self._auth_headers(), "Content-Type": "application/json"},
            retry_on_status=False,
        )
        if response.status_code in (200, 201):
            _logger.debug("Created %s policy for resource=%s group=%s",
                          action, resource_uuid, group_uuid)
            return True
        _logger.error("Failed to create %s policy for resource=%s: %s %s",
                      action, resource_uuid, response.status_code, response.text[:200])
        return False

    # ------------------------------------------------------------------
    # High-level operation
    # ------------------------------------------------------------------

    def replace_policies(self, resource_uuid: str, group_uuid: str, action: str,
                         label: str) -> bool:
        """Replace all *action* policies on a resource with a single group policy."""
        existing_policies = self.get_policies(resource_uuid, action)
        _logger.info("  %s [%s]: found %d existing %s polic(y/ies)",
                     label, resource_uuid, len(existing_policies), action)

        all_deleted = True
        for policy in existing_policies:
            policy_id = policy.get("id")
            if policy_id is not None:
                all_deleted = self.delete_policy(policy_id) and all_deleted

        created = self.create_policy(resource_uuid, group_uuid, action)
        if all_deleted and created:
            _logger.info("  %s [%s]: %s policy set to target group only.",
                         label, resource_uuid, action)
            return True
        _logger.error("  %s [%s]: failed to fully update %s policy.",
                      label, resource_uuid, action)
        return False


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def _clean_handle(raw: str) -> str:
    return raw.strip().strip('"').strip("'").strip()


def parse_handles_csv(raw_handles: str) -> list[str]:
    """Parse comma-separated handles, tolerating quotes and whitespace."""
    return [h for h in (_clean_handle(p) for p in raw_handles.split(",")) if h]


def read_handles_file(path: str) -> list[str]:
    """Read one handle per line; blank lines and '#' comments are ignored."""
    handles: list[str] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.split("#", 1)[0]
            handle = _clean_handle(line)
            if handle:
                handles.append(handle)
    return handles


def collect_handles(args: argparse.Namespace) -> list[str]:
    """Merge handles from --handles, repeated --handle, and --handles-file."""
    merged: list[str] = []
    if args.handles:
        merged.extend(parse_handles_csv(args.handles))
    if args.handle:
        merged.extend(h for h in (_clean_handle(x) for x in args.handle) if h)
    if args.handles_file:
        merged.extend(read_handles_file(args.handles_file))
    # De-duplicate while preserving order.
    seen: set[str] = set()
    unique: list[str] = []
    for handle in merged:
        if handle not in seen:
            seen.add(handle)
            unique.append(handle)
    return unique


def process_handle(
    client: DSpaceClient,
    handle: str,
    group_uuid: str,
    action: str,
    apply_to: str,
    continue_on_bitstream_error: bool = False,
) -> dict[str, Any]:
    """Process one handle and replace policies on bundle and/or bitstreams."""
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

    do_bundle = apply_to in ("bundle", "both")
    do_bitstreams = apply_to in ("bitstreams", "both")

    bitstreams: list = []
    if do_bitstreams:
        bitstreams = client.get_bitstreams(bundle_uuid)
        _logger.info("  Found %d bitstream(s) in ORIGINAL bundle.", len(bitstreams))
        result["bitstreams_attempted"] = len(bitstreams)

    if do_bundle:
        if not client.replace_policies(bundle_uuid, group_uuid, action, "ORIGINAL bundle"):
            result["reason"] = "bundle_policy_update_failed"
            return result

    failed_bitstreams = 0
    for bitstream in bitstreams:
        bitstream_uuid = bitstream["uuid"]
        bitstream_name = bitstream.get("name", bitstream_uuid)
        if not client.replace_policies(
            bitstream_uuid, group_uuid, action, f"bitstream '{bitstream_name}'",
        ):
            failed_bitstreams += 1
            result["bitstreams_failed"] = failed_bitstreams
            if continue_on_bitstream_error:
                _logger.warning(
                    "  bitstream '%s' [%s]: failed; continuing due to "
                    "--continue-on-bitstream-error", bitstream_name, bitstream_uuid,
                )
                continue
            result["reason"] = "bitstream_policy_update_failed"
            _logger.info("Handle %s summary: bitstreams attempted=%d, failed=%d",
                         handle, result["bitstreams_attempted"], result["bitstreams_failed"])
            return result

    if failed_bitstreams > 0:
        _logger.warning("Handle %s finished with %d failed bitstream policy update(s).",
                        handle, failed_bitstreams)
        result["reason"] = "bitstream_policy_update_failed"
        _logger.info("Handle %s summary: bitstreams attempted=%d, failed=%d",
                     handle, result["bitstreams_attempted"], result["bitstreams_failed"])
        return result

    _logger.info("Handle %s processed successfully.", handle)
    result["success"] = True
    _logger.info("Handle %s summary: bitstreams attempted=%d, failed=%d",
                 handle, result["bitstreams_attempted"], result["bitstreams_failed"])
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replace resource policies on items' ORIGINAL bundle/bitstreams "
                    "(by handle) with a single policy for a configurable group/action."
    )
    handles_group = parser.add_mutually_exclusive_group(required=True)
    handles_group.add_argument(
        "--handles",
        help='Comma-separated item handles, e.g. "12345/1001,12345/1002". '
             "Quotes around each handle are optional.",
    )
    handles_group.add_argument(
        "--handle", action="append",
        help="Single item handle. Repeat this argument to pass multiple handles.",
    )
    handles_group.add_argument(
        "--handles-file",
        help="Path to a text file with one handle per line "
             "(blank lines and '#' comments ignored).",
    )

    parser.add_argument(
        "--base-url", required=True,
        help="DSpace REST base URL, e.g. https://example-dspace.invalid/server",
    )
    parser.add_argument(
        "--bearer-token",
        help="Pre-obtained Bearer JWT. If given, it is used directly and the "
             "email/password login is skipped. Falls back to the DSPACE_BEARER_TOKEN "
             "environment variable.",
    )
    parser.add_argument(
        "--email",
        help="Admin account e-mail used for login. Required unless --bearer-token "
             "(or DSPACE_BEARER_TOKEN) is provided.",
    )
    parser.add_argument(
        "--password",
        help="Admin password (used with --email). If omitted, read from the "
             "DSPACE_PASSWORD environment variable, otherwise prompted interactively "
             "(avoids shell history leaks).",
    )

    parser.add_argument(
        "--group", default="Administrator",
        help="Target group: exact group name or group UUID. Default: 'Administrator'.",
    )
    parser.add_argument(
        "--action", default="READ", choices=KNOWN_ACTIONS,
        help="Policy action to replace and create. Default: READ.",
    )
    parser.add_argument(
        "--apply-to", default="bundle", choices=["bundle", "bitstreams", "both"],
        help="Which resources to update: the ORIGINAL bundle only, its bitstreams "
             "only, or both. Default: bundle.",
    )

    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be done without modifying any policies.",
    )
    parser.add_argument(
        "--timeout-sec", type=float, default=DEFAULT_TIMEOUT_SEC,
        help=f"HTTP request timeout in seconds (default: {DEFAULT_TIMEOUT_SEC}).",
    )
    parser.add_argument(
        "--retry-count", type=int, default=DEFAULT_RETRY_COUNT,
        help=f"Retries for retryable HTTP failures (default: {DEFAULT_RETRY_COUNT}).",
    )
    parser.add_argument(
        "--retry-backoff-sec", type=float, default=DEFAULT_RETRY_BACKOFF_SEC,
        help="Base delay (seconds) for exponential retry backoff "
             f"(default: {DEFAULT_RETRY_BACKOFF_SEC}).",
    )
    parser.add_argument(
        "--continue-on-bitstream-error", action="store_true",
        help="Continue with remaining bitstreams in a handle if one fails "
             "(default: stop processing that handle on the first failure).",
    )
    parser.add_argument("--log-file", help="Optional path to also write logs to a file.")
    return parser


def resolve_password(args: argparse.Namespace) -> str:
    if args.password:
        return args.password
    env_password = os.environ.get("DSPACE_PASSWORD")
    if env_password:
        _logger.info("Using password from DSPACE_PASSWORD environment variable.")
        return env_password
    return getpass.getpass(f"Password for {args.email}: ")


def main() -> None:
    args = build_parser().parse_args()
    init_logging(_logger, args.log_file)

    if args.timeout_sec <= 0:
        _logger.error("--timeout-sec must be > 0")
        sys.exit(1)
    if args.retry_count < 0:
        _logger.error("--retry-count must be >= 0")
        sys.exit(1)
    if args.retry_backoff_sec < 0:
        _logger.error("--retry-backoff-sec must be >= 0")
        sys.exit(1)

    handles = collect_handles(args)
    if not handles:
        _logger.error("No valid handles were provided.")
        sys.exit(1)

    token = args.bearer_token or os.environ.get("DSPACE_BEARER_TOKEN")
    if not token and not args.email:
        _logger.error(
            "No authentication provided: pass --bearer-token (or the DSPACE_BEARER_TOKEN "
            "env var), or --email for credential login."
        )
        sys.exit(1)
    auth_mode = "bearer-token" if token else f"login:{args.email}"

    _logger.info(
        "Arguments: base_url=%s, auth=%s, group=%s, action=%s, apply_to=%s, "
        "dry_run=%s, timeout_sec=%s, retry_count=%s, retry_backoff_sec=%s, "
        "continue_on_bitstream_error=%s",
        args.base_url, auth_mode, args.group, args.action, args.apply_to,
        args.dry_run, args.timeout_sec, args.retry_count, args.retry_backoff_sec,
        args.continue_on_bitstream_error,
    )
    _logger.info("Loaded %d handle(s).", len(handles))

    client = DSpaceClient(
        base_url=args.base_url,
        dry_run=args.dry_run,
        timeout_sec=args.timeout_sec,
        retry_count=args.retry_count,
        retry_backoff_sec=args.retry_backoff_sec,
    )

    success_count = 0
    failure_count = 0
    total_bitstreams_attempted = 0
    total_bitstreams_failed = 0
    failure_reasons: dict[str, int] = {}

    try:
        if token:
            client.use_bearer_token(token)
        else:
            client.login(args.email, resolve_password(args))
        group_uuid = client.resolve_group_uuid(args.group)

        for handle in handles:
            handle_result = process_handle(
                client, handle, group_uuid, args.action, args.apply_to,
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
        _logger.info("Done. %d handle(s) processed successfully, %d skipped/failed.",
                     success_count, failure_count)
        _logger.info("Bitstream summary: attempted=%d, failed=%d",
                     total_bitstreams_attempted, total_bitstreams_failed)
        if failure_reasons:
            _logger.info("Aggregate failure report:")
            for reason, count in sorted(failure_reasons.items()):
                _logger.info("  %s: %d", reason, count)
    finally:
        client.logout()

    if failure_count > 0 or total_bitstreams_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
