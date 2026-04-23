# Bank CSV Normalizer

Ingests bank-exported CSVs, validates and normalizes transaction data,
deduplicates against a persistent per-account index, and emits a unified
output format. Runs unattended via cron or systemd timer.

## Tech Stack

- **Python 3.10+** — processing engine ([engine/](engine/))
- **Bash** — cron-safe orchestrator with lockfile and size-stability check
- **YAML** — per-bank configuration ([config/](config/))
- **Dependencies** — Python stdlib + `pyyaml` only (no build step)

## Key Directories

| Path | Purpose |
|------|---------|
| [engine/process_csv.py](engine/process_csv.py) | Pipeline entry point; orchestrates all stages. `main()` + `NORMALIZED_FIELDNAMES` |
| [engine/core/](engine/core/) | Shared pipeline modules: `csv_runtime`, `csv_validation`, `duplicate_index`, `completion`, `runtime` |
| [engine/banks/](engine/banks/) | One sub-package per bank. Each must export `normalize_row()` |
| [engine/banks/fintro/](engine/banks/fintro/) | Reference bank: `normalize_row`, `extract_details`, `parsers`, `reconcile` |
| [config/](config/) | `<bank>.yaml` configs (bank name is the filename) + `app.env` (EMAIL_TO) |
| [bank-csv-normalizer.bash](bank-csv-normalizer.bash) | Cron entry; lockfile, size-stability check, invokes Python module |
| `data/incoming/` | Drop CSVs here to trigger processing |
| `data/normalized/` | Successful normalized output (timestamped) |
| `data/processed/` | Originals after processing (success / partial / failed) |
| `data/failed/` | Rows that failed normalization or dedup |
| `data/duplicate-index/` | Per-account persistent dedup index + rotated backups |
| `data/logs/` | Per-run timestamped logs |
| `data/temp/` | Working files; cleaned up after each run |

## Running

```bash
# Manual single run (processes all CSVs in data/incoming/)
./bank-csv-normalizer.bash

# Cron (every 5 min)
*/5 * * * * /path/to/bank-csv-normalizer.bash

# Direct Python invocation (debugging)
PYTHONPATH=. python3 -m engine.process_csv <csv_path> <YYYYMMDD-HHMMSS> <logfile_path>
```

## Lint

```bash
ruff check .
```
Config: [ruff.toml](ruff.toml) — line-length 120, py310 target, selects `E,F,W,I,UP,B`.

No test suite. Verify behavior by placing a sample CSV in `data/incoming/`
and inspecting `data/normalized/`, `data/failed/`, and `data/logs/`.

## Exit Codes

Defined in [engine/process_csv.py](engine/process_csv.py) and
[engine/core/completion.py](engine/core/completion.py):

| Code | Outcome |
|------|---------|
| `0` | success or all_full_duplicates |
| `65` | structure_failed / all_failed |
| `75` | partial (some rows failed) |
| `92–97` | critical file operation errors |
| `99` | unexpected exception |

## Adding a New Bank

1. Create `config/<bank>.yaml` — required/optional columns, regex rules,
   filter values, `duplicate_key`. See [config/fintro.yaml](config/fintro.yaml).
   Bank name is derived from the filename by `load_all_bank_configs()`
   in [engine/core/csv_runtime.py](engine/core/csv_runtime.py) — no `bank:` field needed.
2. Create `engine/banks/<bank>.py` (or `engine/banks/<bank>/` package
   exposing `normalize_row` in `__init__.py`). See [engine/banks/fintro/](engine/banks/fintro/).
3. No further code changes — `autodetect_bank()` in
   [engine/core/csv_validation.py](engine/core/csv_validation.py) matches configs by
   header, and `process_csv.py` imports the bank module dynamically via
   `importlib.import_module(f"engine.banks.{bank_name}")`.

## Hooks / Settings

[.claude/settings.local.json](.claude/settings.local.json) only whitelists
`ruff check`, `pre-commit run`, and `git add`/`git commit` for permission
prompts. No PreToolUse/PostToolUse hooks are configured — the agent's
workflow is not modified by the harness.

## Additional Documentation

See [.claude/rules/architectural_patterns.md](.claude/rules/architectural_patterns.md)
for design patterns, pipeline phases, reconciliation strategy, and error
criticality tiers.
