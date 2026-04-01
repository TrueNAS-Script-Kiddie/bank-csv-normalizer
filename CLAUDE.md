# Bank CSV Normalizer

Automated pipeline that ingests bank-exported CSVs, validates and normalizes
transaction data, deduplicates against a persistent index, and emits a unified
output format. Designed to run unattended via cron or systemd timer.

## Tech Stack

- **Python 3.10+** — processing engine (`engine/`)
- **Bash** — orchestration, lockfile, cron-safe entry point
- **YAML** — per-bank configuration (`config/*.yaml`)
- **stdlib only** — `csv`, `re`, `unicodedata`, `yaml`, `shutil`, `subprocess`

## Key Directories

| Path | Purpose |
|------|---------|
| `engine/core/` | Shared pipeline modules (validation, dedup, I/O, completion) |
| `engine/banks/` | One module per bank — each exports `normalize_row()` |
| `engine/process_csv.py` | Pipeline entry point; orchestrates all stages |
| `config/` | Per-bank YAML configs + `app.env` (EMAIL_TO) |
| `data/incoming/` | Drop CSVs here to trigger processing |
| `data/normalized/` | Successful normalized output |
| `data/processed/` | Originals after processing (success/partial/failed) |
| `data/failed/` | Rows that failed normalization or dedup |
| `data/duplicate-index/` | Persistent dedup index + timestamped backups |
| `data/logs/` | Per-run timestamped logs |
| `data/temp/` | Working files; cleaned up after each run |

## Running

```bash
# Manual single run (processes all CSVs in data/incoming/)
./bank-csv-normalizer.bash

# Cron (every 5 min)
*/5 * * * * /path/to/bank-csv-normalizer.bash

# Direct Python invocation (for debugging)
PYTHONPATH=. python3 -m engine.process_csv <csv_path> <YYYYMMDD-HHMMSS> <logfile_path>
```

No build step. No package install needed beyond Python stdlib + `pyyaml`.

## Exit Codes

Defined in [engine/process_csv.py](engine/process_csv.py) (outcome classification block):

| Code | Outcome |
|------|---------|
| `0` | success or all_full_duplicates |
| `65` | structure_failed / all_failed |
| `75` | partial (some rows failed) |
| `92–97` | critical file operation errors |
| `99` | unexpected exception |

## Adding a New Bank

1. Create `config/<bank>.yaml` — define required columns, regex rules, filter
   values, and `duplicate_key` extraction. See [config/fintro.yaml](config/fintro.yaml) as reference.
   The bank name is derived from the filename; no `bank:` field needed in the YAML.
2. Create `engine/banks/<bank>.py` — implement `normalize_row(csv_row)`.
   See [engine/banks/fintro.py](engine/banks/fintro.py) as reference.
3. No further code changes needed — `autodetect_bank()` loads all `config/*.yaml`
   automatically ([engine/core/csv_runtime.py](engine/core/csv_runtime.py#L175)) and
   `process_csv.py` imports the bank module dynamically after detection.

## Additional Documentation

| File | When to check |
|------|--------------|
| [.claude/docs/architectural_patterns.md](.claude/docs/architectural_patterns.md) | Understanding design decisions, pipeline phases, normalization strategy, error handling conventions |
