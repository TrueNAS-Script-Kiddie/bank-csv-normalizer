# Bank CSV Normalizer

Automated pipeline for ingesting bank-exported CSVs, validating them,
normalizing transactions into a unified model, and deduplicating against a
persistent per-account index. Designed for unattended cron / systemd timer
execution on TrueNAS or any Linux host.

## Overview

- Detects incoming CSVs in `data/incoming/`
- Auto-detects the bank by matching CSV headers against YAML configs
- Validates, filters and normalizes each row
- Deduplicates via a per-account persistent index
- Moves originals to `data/processed/` and emits normalized output to
  `data/normalized/`
- Sends a TrueNAS `mail.send` notification per run

## Project Structure

```
bank-csv-normalizer/
├── bank-csv-normalizer.bash       # Cron entry (lockfile, size-stability check)
├── engine/
│   ├── process_csv.py             # Pipeline entry point
│   ├── core/                      # csv_runtime, csv_validation,
│   │                              # duplicate_index, completion, runtime
│   └── banks/
│       └── fintro/                # Per-bank package: normalize_row,
│                                  # extract_details, parsers, reconcile
├── config/
│   ├── fintro.yaml                # Per-bank config (columns, regex, dedup)
│   └── app.env                    # EMAIL_TO
├── data/
│   ├── incoming/ normalized/ processed/ failed/
│   ├── duplicate-index/           # Per-account index + rotated backups
│   ├── logs/ temp/
├── ruff.toml
└── .vscode/sftp.json              # Optional auto-sync to remote host
```

## How It Works

1. Bash script runs (cron, systemd timer, or manually) and takes a lockfile.
2. For each CSV in `data/incoming/`:
   - Waits 2 s and compares file size twice; skips the file if still growing
     (guards against partial SFTP uploads).
   - Creates a timestamped logfile in `data/logs/`.
   - Invokes `python3 -m engine.process_csv <csv> <timestamp> <logfile>`.
3. The Python engine loads the CSV, auto-detects the bank, validates and
   maps columns, loads the account-specific duplicate index, and processes
   each row: dedup → normalize → write temp output.
4. A single exit path (`completion.finalize`) moves the original CSV,
   commits the updated duplicate index, moves the normalized output, rotates
   backups, cleans the temp dir, logs, emails, and exits with an outcome
   code (`0`, `65`, `75`, `92–97`, `99`).

## Requirements

- Python 3.10+
- `pyyaml` (all other runtime deps are stdlib)
- Bash, `stat`, `mv`
- Optional: `/usr/bin/midclt` for TrueNAS email notifications
  (see `engine/core/runtime.py::send_email`)

## Running

Manual:

```bash
./bank-csv-normalizer.bash
```

Cron (every 5 minutes):

```
*/5 * * * * /path/to/bank-csv-normalizer.bash
```

Direct (for debugging):

```bash
PYTHONPATH=. python3 -m engine.process_csv <csv_path> <YYYYMMDD-HHMMSS> <logfile_path>
```

## Lint

```bash
ruff check .
```

Configured in `ruff.toml` (line-length 120, py310 target,
selects `E,F,W,I,UP,B`).

## Adding a New Bank

1. Drop a `config/<bank>.yaml` defining required columns, regex rules,
   filter values and `duplicate_key`. See `config/fintro.yaml` as reference.
2. Add an `engine/banks/<bank>.py` module (or an `engine/banks/<bank>/`
   package exposing `normalize_row` in `__init__.py`) that implements
   `normalize_row(csv_row) -> dict`.
3. Nothing else to wire up — `autodetect_bank()` matches by CSV header and
   `process_csv.py` imports the bank module dynamically.

## VS Code SFTP Sync

`.vscode/sftp.json` enables automatic upload on save to the deployment host.
Update `host`, `username`, `privateKeyPath`, and `remotePath` to match your
environment.

## License

Private project — no public license.
