# Bank CSV Normalizer

Ingests bank-exported CSVs, validates and normalizes transaction data,
deduplicates against a persistent per-account index, emits a unified
output format, and imports that output into Firefly III via its REST API.
Runs unattended from a TrueNAS cron job.

## Tech Stack

- **Python 3.10+** — processing engine ([engine/](engine/))
- **Bash** — cron-safe orchestrator with `flock` and upload check (ctime ≥ 30 s + complete last line); idle runs use builtins only
- **YAML** — per-bank configuration ([config/](config/))
- **Dependencies** — Python stdlib + `pyyaml` only (no build step; the Firefly client uses `urllib`)

## Key Directories

| Path | Purpose |
|------|---------|
| [engine/process_csv.py](engine/process_csv.py) | Normalizer entry point; orchestrates all stages. `main()` + `NORMALIZED_FIELDNAMES` |
| [engine/core/](engine/core/) | Shared pipeline modules: `csv_runtime`, `csv_validation`, `duplicate_index`, `completion`, `runtime` |
| [engine/banks/](engine/banks/) | One sub-package per bank. Each must export `normalize_row()` |
| [engine/banks/fintro/](engine/banks/fintro/) | Reference bank: `normalize_row`, `extract_details`, `parsers`, `reconcile` |
| [engine/firefly/](engine/firefly/) | Firefly III import: `api` (REST client), `import_normalized` (importer) |
| [config/](config/) | `<bank>.yaml` configs (bank name is the filename) + `app.env` (`FIREFLY_URL`, `FIREFLY_TOKEN`) |
| [bank-csv-normalizer.bash](bank-csv-normalizer.bash) | Cron entry; `flock`, upload check (ctime ≥ 30 s + complete last line), normalizes each incoming CSV, then runs the importer |
| `bank-csv-originals/` | Backup of every unique bank export; source for regenerating `data/` |
| `data/incoming/` | Drop CSVs here to trigger processing |
| `data/normalized/` | Normalized output waiting for import (timestamped) |
| `data/imported/` | Normalized files after import: `<ts>-<name>-imported.csv` (`-imported-partial` if rows failed) |
| `data/processed/` | Originals after processing (success / partial / failed) |
| `data/failed/` | Rows that failed normalization, dedup, or import; whole files bash moved after a crash |
| `data/duplicate-index/` | Per-account persistent dedup index (successfully normalized rows only) + backups rotated per account |
| `data/logs/` | Per-run logs: `<ts>-<name>.log` (normalizer) and `<ts>-<name>-import.log`; `<ts>` = normalizer run, so all files of one bank CSV sort together |
| `data/temp/` | Working files; cleaned up after each run, except after a critical error (it then holds the index rollback copy) |

The token lives only in the server copy of `config/app.env` (mode 600); the
SFTP watcher excludes that file, so a local copy is never uploaded over it.

## Running

```bash
# Manual single run (normalize data/incoming/, then import data/normalized/)
./bank-csv-normalizer.bash

# TrueNAS cron job: every minute, as the user that owns the folder, "Hide Standard Error" off
# (stderr = alert email), with the encrypted-dataset lock-guard in the Command field

# Normalizer only (debugging)
PYTHONPATH=. python3 -m engine.process_csv <csv_path> <YYYYMMDD-HHMMSS> <logfile_path>

# Importer only; --dry-run builds every request but sends and moves nothing
PYTHONPATH=. python3 -m engine.firefly.import_normalized [--dry-run] [--show N] [csv ...]
```

Alerts go to stderr ([engine/core/runtime.py](engine/core/runtime.py) `alert`);
success is silent.

## Lint

```bash
ruff check .
```
Config: [ruff.toml](ruff.toml) — line-length 120, py310 target, selects `E,F,W,I,UP,B`.

No test suite. Verify the normalizer by placing a sample CSV in `data/incoming/`
and inspecting `data/normalized/`, `data/failed/`, and `data/logs/`; verify
the importer with `--dry-run` on the server (the token lives there).

## Exit Codes

Normalizer — [engine/process_csv.py](engine/process_csv.py) and
[engine/core/completion.py](engine/core/completion.py):

| Code | Outcome |
|------|---------|
| `0` | success or all_full_duplicates |
| `65` | structure_failed / all_failed |
| `75` | partial (some rows failed) |
| `92–97` | critical file operation errors |
| `99` | unexpected exception |

Importer — [engine/firefly/import_normalized.py](engine/firefly/import_normalized.py):
`0` all imported, `75` some rows failed, `69` Firefly unreachable or token refused
(files stay in `data/normalized/`; alerted once per outage via
`data/firefly-import-blocked.flag`).

## Firefly III Import

One `POST /api/v1/transactions` per row (~0.5 s each), with
`error_if_duplicate_hash`, so re-importing a file is safe. Row → split mapping
lives in `build_split()`:

- Sign of `amount` decides withdrawal / deposit; `asset_account_iban` must match
  a Firefly asset account (looked up by IBAN every run).
- Counterparty IBAN of an own asset account → **transfer**, deduplicated by
  "match or create": the first side creates it, the other side claims it
  (same accounts, same amount, ±7 days). Order and history coverage of the
  accounts' CSVs don't matter.
- Empty description → counterparty name → first line of `notes` (Firefly
  requires a description).
- No counterparty → `(onbekend)`, except cash withdrawals (Firefly's Cash
  account). Fintro's own transactions get counterparty `Fintro` in the
  normalizer (`BANK_COUNTERPARTY_TRANSACTION_TYPES`).

Gotchas:

- Firefly's duplicate check includes **deleted** transactions. After deleting
  transactions, also `DELETE /api/v1/data/purge`, or a re-import silently
  counts them as "already present".
- Dates must be ISO; Firefly parses `dd/mm/yyyy` as US month/day without error.
- Personal Access Tokens expire after at most one year → "FIREFLY IMPORT BLOCKED" alert.

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
   `importlib.import_module(f"engine.banks.{bank_name}")`. The importer needs
   no change either, as long as the account exists in Firefly with its IBAN.

## Hooks / Settings

[.claude/settings.local.json](.claude/settings.local.json) only whitelists
`ruff check`, `pre-commit run`, and `git add` for permission prompts. No hooks
are configured at project level.

## Additional Documentation

See [.claude/rules/architectural_patterns.md](.claude/rules/architectural_patterns.md)
for design patterns, pipeline phases, reconciliation strategy, and error
criticality tiers.
