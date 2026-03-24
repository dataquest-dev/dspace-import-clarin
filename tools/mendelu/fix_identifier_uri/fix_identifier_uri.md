# fix_identifier_uri

Normalises `dc.identifier.uri` metadata values across all DSpace items so
that every value uses the canonical handle prefix form defined in
`mendelu_settings.py` (`dspace.handle_prefix[0]`, e.g. `http://hdl.handle.net/`).

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt` at the repository root

## Usage

```bash
python tools/mendelu/fix_identifier_uri/fix_identifier_uri.py [--server URL] [--user USER] [--password PASS] [--dry-run]
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

Tests are pure unit tests and require no DSpace server. To run them locally, install `pytest` (it is a development dependency and may not be included in the default requirements):

```bash
pip install pytest
pytest tools/mendelu/tests/test_fix_identifier_uri.py -v
```
