# Set ORIGINAL bundle resource policies by handle

Standalone tool that, for each given item **handle**, replaces all resource
policies of a chosen **action** (default `READ`) on the item's **ORIGINAL**
bundle and/or its bitstreams with a single policy granting that action to a
configurable **group** (default `Administrator`).

Typical use: embargo the original files of a set of records so only
administrators can read them.

It is fully self-contained: it depends only on `requests` and imports no other
module from this repository (no `src/settings` / `project_settings` / `utils`).

## Install

```bash
python -m venv .venv
. .venv/bin/activate        # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Requires Python 3.8+.

## Security

- Endpoints, handles and credentials are **never** stored in this repo.
- Pass `--base-url` / `--email` at runtime.
- For the password, prefer **not** passing `--password` on the command line
  (it lands in shell history). The script falls back to the `DSPACE_PASSWORD`
  environment variable, then to an interactive prompt.

## Authentication

Two options:

- **Credential login** (default): pass `--email` and let the script fetch the
  Bearer JWT for you (password via `--password`, `DSPACE_PASSWORD`, or prompt).
- **Existing Bearer token**: pass `--bearer-token "<jwt>"` (or set
  `DSPACE_BEARER_TOKEN`). The login step is skipped and the token is used
  directly; the script still fetches a CSRF token (required for DELETE/POST) and
  does **not** call logout (so it won't invalidate a token you reuse).

```bash
# Use a pre-obtained Bearer token instead of email/password
export DSPACE_BEARER_TOKEN="eyJ..."        # or pass --bearer-token "eyJ..."
python set_original_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --handles "12345/1001" --group Anonymous --action READ
```

## Usage

```bash
# Comma-separated handles
python set_original_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --email "admin@example.invalid" \
  --handles "12345/1001,12345/1002,12345/1003"

# Repeated --handle
python set_original_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --email "admin@example.invalid" \
  --handle "12345/1001" --handle "12345/1002"

# Handles from a file (one per line, '#' comments allowed)
python set_original_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --email "admin@example.invalid" \
  --handles-file handles.txt
```

### Configurable group / action

```bash
# Grant READ to a named group instead of Administrator
python set_original_policies.py ... --handles "12345/1001" --group "Embargo Managers"

# Use a group UUID directly (skips the name lookup)
python set_original_policies.py ... --handles "12345/1001" \
  --group "a8980286-7ec9-465c-b696-5dc218968292"

# Replace WRITE policies instead of READ, only on the bundle (not bitstreams)
python set_original_policies.py ... --handles "12345/1001" \
  --action WRITE --apply-to bundle
```

### Always dry-run first

```bash
python set_original_policies.py ... --handles "12345/1001" --dry-run
```

`--dry-run` logs every DELETE/POST it *would* perform without changing anything.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `--handles` / `--handle` / `--handles-file` | — | Handle input (mutually exclusive, one required). |
| `--base-url` | — | DSpace REST base URL (`.../server`). Required. |
| `--bearer-token` | env | Pre-obtained Bearer JWT (else `DSPACE_BEARER_TOKEN`). Skips login. |
| `--email` | — | Admin login e-mail. Required unless a Bearer token is given. |
| `--password` | env/prompt | Admin password (else `DSPACE_PASSWORD` env var, else prompt). |
| `--group` | `Administrator` | Target group name **or** UUID. |
| `--action` | `READ` | One of READ, WRITE, ADD, REMOVE, DELETE, ADMIN. |
| `--apply-to` | `both` | `bundle`, `bitstreams`, or `both`. |
| `--dry-run` | off | Simulate without modifying policies. |
| `--timeout-sec` | `30` | Per-request HTTP timeout. |
| `--retry-count` | `3` | Retries on retryable failures (timeouts, 5xx, 429). |
| `--retry-backoff-sec` | `1.0` | Base delay for exponential backoff. |
| `--continue-on-bitstream-error` | off | Keep going if one bitstream fails. |
| `--log-file` | — | Also write logs to this file. |

Exit code is non-zero if any handle or bitstream update failed.

## API flow

Authentication uses the DSpace 7/8 Bearer-JWT flow (same as
[dataquest-dev/dspace-import-clarin PR #264](https://github.com/dataquest-dev/dspace-import-clarin/pull/264)):

1. `GET  /api/security/csrf` — obtain the XSRF token.
2. `POST /api/authn/login` — read the JWT from the `Authorization: Bearer <jwt>` header. *(Skipped when `--bearer-token` / `DSPACE_BEARER_TOKEN` is supplied.)*
3. Send `Authorization: Bearer <jwt>` + `X-XSRF-TOKEN` on every subsequent request.
4. `GET /api/eperson/groups/search/byMetadata?query=<group>` — resolve the target group (skipped if a UUID is given).
5. Resolve handle via `GET /api/pid/find`, falling back to `GET /api/discover/search/objects`.
6. `GET` the item's ORIGINAL bundle and its bitstreams.
7. Per target resource: `GET` existing policies for the action, `DELETE` them, `POST` one new policy for the target group.
8. `POST /api/authn/logout`. *(Skipped when an externally provided Bearer token is used.)*
