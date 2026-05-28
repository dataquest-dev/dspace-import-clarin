# Set READ Resource Policies to Administrator

## Goal
Set or replace all READ resource policies on each item's ORIGINAL bundle and its bitstreams so only the Administrator group has READ access.

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

## Optional Resilience Flags
- --timeout-sec <seconds>
- --retry-count <n>
- --retry-backoff-sec <seconds>
- --continue-on-bitstream-error
- --dry-run

## API Flow
1. GET /api/security/csrf
2. POST /api/authn/login
3. GET /api/eperson/groups/search/byMetadata?query=Administrator
4. Resolve handle with /api/pid/find, fallback to /api/discover/search/objects
5. GET item ORIGINAL bundle and bitstreams
6. For each target resource:
- GET existing READ policies
- DELETE existing READ policies
- POST new READ policy for Administrator group
7. POST /api/authn/logout

## Output Summary
- Per handle: bitstreams attempted and failed.
- Final run: total bitstreams attempted and failed.
- Final run: aggregate failure counts by reason.
