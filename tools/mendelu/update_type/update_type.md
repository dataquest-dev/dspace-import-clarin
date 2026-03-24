# update_type

Maps `dc.type` metadata values across all DSpace items using a configurable
lookup table.  The default mapping is:

| source value | target value |
|---|---|
| `J_ČLÁNEK | `article` |

## Usage

```bash
python tools/mendelu/update_type/update_type.py [--server URL] [--user USER] [--password PASS] [--dry-run]
```

Credentials and server URL are read from `tools/mendelu/.env` by default (one level up from the script).
If command-line arguments cannot be used (e.g. in automated pipelines), edit `.env` directly:

```
DSPACE_ENDPOINT=https://dspace.mendelu.cz/server/api
DSPACE_USER=admin@mendelu.cz
DSPACE_PASSWORD=secret
```

### Options

| Option | Description |
|---|---|
| `--server URL` | DSpace REST API endpoint (default: `DSPACE_ENDPOINT` from `.env`) |
| `--user USER` | Admin username (default: `DSPACE_USER` from `.env`) |
| `--password PASS` | Admin password (default: `DSPACE_PASSWORD` from `.env`) |
| `--dry-run` | Log what would change without writing anything to DSpace |

## Running tests

Tests are pure unit tests and require no DSpace server:

```bash
pytest tools/mendelu/tests/test_update_type.py -v
```
