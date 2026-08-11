# Set READ Resource Policies to a Group

## Goal
Set or replace all READ resource policies on each item's ORIGINAL bundle and its bitstreams so only a single chosen group has READ access. The default group is `Administrator`; pass `--group Anonymous` to make the files publicly readable.

## Security Rules
- Do not store endpoints, handles, usernames, or passwords in this repository.
- Pass endpoint and credentials only through script arguments at runtime.
- Use made-up placeholder values in documentation and examples.

## Script
- Path: tools/replace_resource_policies/set_admin_policies.py

## Input Mode
Handles are passed directly in arguments.

1. Comma-separated handles:
python tools/replace_resource_policies/set_admin_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --email "admin@example.invalid" \
  --password "CHANGE_ME" \
  --handles "12345/1001,12345/1002,12345/1003"

2. Repeated handle argument:
python tools/replace_resource_policies/set_admin_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --email "admin@example.invalid" \
  --password "CHANGE_ME" \
  --handle "12345/1001" \
  --handle "12345/1002"

## Authentication
Two options:

1. User/password (default): pass --email and --password.

2. Bearer token (for Shibboleth/SAML-only instances): the project has no Shibboleth
   login flow, but the REST API authenticates every call with a JWT, so you can reuse a
   token from a browser session:
   - Log in to the DSpace UI in the browser via Shibboleth.
   - Open DevTools -> Network, click any /api/... request, and copy the value of the
     "Authorization: Bearer <token>" request header (the part after "Bearer ").
   - Pass it as --bearer-token "<token>" instead of --email/--password.
   - The token is short-lived (DSpace default ~30 min), so run the script promptly.

Example (Shibboleth-only instance, set handles public for Anonymous):
python tools/replace_resource_policies/set_admin_policies.py --base-url "https://example-dspace.invalid/server" --bearer-token "eyJhbGci...REDACTED" --handles "12345/1001,12345/1002" --group "Anonymous" --dry-run

## Target Group
- --group <name>  Exact group name that receives the READ policy. Default: "Administrator".
  Use --group "Anonymous" to make the files publicly readable.

Example (make handles public for Anonymous):
python tools/replace_resource_policies/set_admin_policies.py \
  --base-url "https://example-dspace.invalid/server" \
  --email "admin@example.invalid" \
  --password "CHANGE_ME" \
  --handles "12345/1001,12345/1002" \
  --group "Anonymous"

## Optional Resilience Flags
- --timeout-sec <seconds>
- --retry-count <n>
- --retry-backoff-sec <seconds>
- --continue-on-bitstream-error
- --dry-run

## API Flow
1. GET /api/security/csrf
2. POST /api/authn/login
3. GET /api/eperson/groups/search/byMetadata?query=<group> (default Administrator, e.g. Anonymous)
4. Resolve handle with /api/pid/find, fallback to /api/discover/search/objects
5. GET item ORIGINAL bundle and bitstreams
6. For each target resource:
- GET existing READ policies
- DELETE existing READ policies
- POST new READ policy for the target group
7. POST /api/authn/logout

## Output Summary
- Per handle: bitstreams attempted and failed.
- Final run: total bitstreams attempted and failed.
- Final run: aggregate failure counts by reason.
