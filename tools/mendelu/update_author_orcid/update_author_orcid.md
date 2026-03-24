# update_author_orcid

Sets the `dc.contributor.author` **authority** field to the corresponding
ORCID identifier for every author that appears in the item's
`dc.identifier.orcid` metadata.

### Expected format of `dc.identifier.orcid` values

Each value must follow the layout `Surname, Given Name 0000-0002-1234-567X`
— the author name is everything before the `XXXX-XXXX-XXXX-XXXX` pattern.
Entries without a recognisable ORCID are skipped; authors whose name has no
matching ORCID entry are left unchanged.

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt` at the repository root

## Usage

```bash
python tools/mendelu/update_author_orcid/update_author_orcid.py [--server URL] [--user USER] [--password PASS] [--dry-run]
```

Credentials and server URL are read from `tools/mendelu/.env` by default.
Edit `.env` directly when command-line arguments are not suitable:

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
| `--no-orcid-check` | Skip the HTTP resolution check for each ORCID (faster, avoids ORCID rate limits) |

## Running tests

Tests are pure unit tests and require no DSpace server. They use `pytest`, which is not part of the default runtime requirements:

```bash
pip install pytest
pytest tools/mendelu/tests/test_update_author_orcid.py -v
```
