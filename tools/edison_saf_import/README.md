# Edison SAF Import to DSpace

A modular Python package for importing Edison exports into DSpace via Docker containers.

## Project Structure

```
edison_saf_import/
├── __init__.py          # Package initialization
├── config.py            # Configuration constants and settings
├── notifier.py          # Email notification functionality  
├── importer_logic.py    # Core import logic and Docker operations
├── main.py              # Main entry point and orchestration
├── run.py               # Backward compatibility script
├── requirements.txt     # Package dependencies
└── README.md            # This documentation
```

## Installation

Install the required dependency:

```bash
pip install tqdm>=4.64.0
```

Or install from requirements file:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python -m edison_saf_import.main
```

### With Verbose Output

```bash
python -m edison_saf_import.main --verbose
```

### With Email Reports

```bash
python -m edison_saf_import.main --email --email-to admin@example.com
```

### All Options

```bash
python -m edison_saf_import.main \
    --verbose \
    --email \
    --email-to admin@example.com user@example.com \
    --email-from edison@example.com \
    --smtp-server smtp.example.com \
    --smtp-port 587 \
    --smtp-username user \
    --smtp-password pass
```

## Configuration

Edit `config.py` to customize:

- **Paths**: Export paths, container paths, mapfile locations
- **Docker**: Container name and configuration
- **DSpace**: ePerson and collection mappings
- **Email**: SMTP server settings and default recipients
- **Timeouts**: Command execution timeouts

## Classes

### EdisonImporter

Main class handling:
- Docker container operations
- Export directory discovery
- DSpace SAF imports
- Error tracking and logging

### EmailNotifier

Handles:
- Email report generation
- SMTP communication
- Import status summaries

### ProgressTracker

Provides:
- Progress bars using `tqdm` library
- Real-time status updates with time estimates
- Collection-by-collection progress tracking
- Graceful fallback when `tqdm` not available

## Error Handling

- Timeout protection for all Docker commands
- Comprehensive error message collection
- Graceful failure handling with cleanup
- Detailed logging for debugging

## Backward Compatibility

Use `run.py` for direct script execution:

```bash
python run.py --verbose
```
