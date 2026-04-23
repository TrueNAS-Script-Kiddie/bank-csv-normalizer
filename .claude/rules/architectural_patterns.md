---
paths:
  - engine/**
  - config/**
  - bank-csv-normalizer.bash
---

# Architectural Patterns

## 1. Linear Multi-Phase Pipeline

[engine/process_csv.py](../../engine/process_csv.py) `main()` runs a strict
ordered sequence. Each phase must succeed before the next begins:

1. **Load** — encoding/delimiter auto-detection (`load_csv_rows` in
   [engine/core/csv_runtime.py](../../engine/core/csv_runtime.py)).
2. **Autodetect bank** — match CSV headers against all loaded YAML configs
   (`autodetect_bank` in [engine/core/csv_validation.py](../../engine/core/csv_validation.py)).
3. **Validate & map** — remap CSV columns to internal names, apply `filter` /
   `filter_regex`, then `regex` validation (`validate_and_prepare`).
4. **Resolve account-specific dedup-index path** — from `duplicate_key.partition_by`.
5. **Load dedup index** — per-account persistent CSV
   (`load_duplicate_index` in [engine/core/duplicate_index.py](../../engine/core/duplicate_index.py)).
6. **Per-row loop** — `extract_duplicate_key` → `classify_duplicate` → bank's
   `normalize_row` → write temp output.
7. **Outcome classification** — `success` / `partial` / `all_failed` /
   `all_full_duplicates` / `structure_failed` / `error`.
8. **Finalize** — single exit path for all file moves, index commit, email
   (`finalize` in [engine/core/completion.py](../../engine/core/completion.py)).

## 2. Single Exit Path (`completion.finalize`)

`finalize()` is the **only** place that calls `sys.exit()`. Every code path
(normal, error, structure failure, row-level failure) calls it, guaranteeing
that file moves, index commits, writer cleanup, backup rotation, temp cleanup,
and email always happen together. Outcomes map 1:1 to exit codes and
destination subdirectories — see the step-numbered blocks inside `finalize`.

## 3. Configuration-Driven Bank Support

All bank-specific behaviour lives in `config/<bank>.yaml`. The engine has
zero hardcoded bank names. Key YAML sections (see [config/fintro.yaml](../../config/fintro.yaml)):

- `columns.required` — expected headers with `names` (aliases), `regex`
  (per-cell validation), `filter` (exact-match allowlist), `filter_regex`.
- `columns.optional` — headers included when present, ignored when absent.
- `duplicate_key.columns` / `duplicate_key.regex` — how to extract the dedup
  key from a row.
- `duplicate_key.partition_by` — internal column whose value names the
  per-account index file (`<VALUE>-duplicate-index.csv`). Falls back to
  `<bank>-duplicate-index.csv` if absent.

Bank name is derived from the YAML filename and injected as `cfg["bank"]` by
`load_all_bank_configs` in [engine/core/csv_runtime.py](../../engine/core/csv_runtime.py).
The normalizer module is loaded dynamically by `process_csv.py` via
`importlib.import_module(f"engine.banks.{bank_name}")`.

## 4. Lazy Writer Pattern

CSV writers are never opened speculatively. Each output file
(`temp_normalized`, `failed_normalize`, `failed_duplicate`) holds a
`{"writer": None, "file": None}` ref dict. `ensure_writer()` in
[engine/core/csv_runtime.py](../../engine/core/csv_runtime.py) creates the file +
writer on first use and writes the header once.

All refs are collected in `context["open_writers"]` for guaranteed cleanup by
`close_open_writers()` in [engine/core/completion.py](../../engine/core/completion.py).

## 5. Two-Source Data Reconciliation

Fintro CSVs carry many fields in both a dedicated column and inside the
free-text `details` column. The normalizer compares both sources and either
merges them or raises `ValueError` on mismatch — neither source is blindly
trusted.

Precedence rules live in [engine/banks/fintro/normalize_row.py](../../engine/banks/fintro/normalize_row.py):

- **CSV wins** for amount, IBAN, dates, and free-text messages (details
  validates).
- **Details wins** for structured references, BIC, and fields missing from
  the CSV column.
- **Structured messages** (Belgian `+++xxx/xxxx/xxxxx+++` format) take
  priority over free-text when either source has one
  (`extract_structured_ref` in [engine/banks/fintro/parsers.py](../../engine/banks/fintro/parsers.py)).

`merge_opposing_account_name` and `reconcile_transaction_types` in
[engine/banks/fintro/reconcile.py](../../engine/banks/fintro/reconcile.py) encode the
field-by-field rules.

## 6. Two-Phase `normalize_row` + Sequential `details` Parsing

`normalize_row()` in [engine/banks/fintro/normalize_row.py](../../engine/banks/fintro/normalize_row.py)
is split into two explicit phases:

**Phase 1 — Extraction.** Pull all values into named variables; no output is
written yet.
- *1a* — individual dedicated columns are pulled by key from `csv_row`.
- *1b* — `extract_details()` in [engine/banks/fintro/extract_details.py](../../engine/banks/fintro/extract_details.py)
  parses the free-text `details` column in two sub-passes:
  1. Easy-to-detect postfixes anchored with `$` are stripped first
     (VALUTADATUM, BANKREFERENTIE, UITGEVOERD OP).
  2. The remainder is matched by leading pattern anchored with `^` to
     identify transaction type and extract the rest (STORTING, DOORLOPENDE
     OPDRACHT, DOMICILIERING, OVERSCHRIJVING, BETALING, MOBIELE BETALING,
     GELDOPNEMING, old-card fallback).

  Each matched segment is removed from `remaining_details`. Anything left at
  the end raises `ValueError`.

**Phase 2 — Reconcile, reformat, assemble.** No regex or string parsing at
this stage; only cross-source decisions, cosmetic replacement (via the
`REPLACE_IN_*` tables in `normalize_row.py`), card-number masking, and final
assembly of the 13 `NORMALIZED_FIELDNAMES` defined in
[engine/process_csv.py](../../engine/process_csv.py).

## 7. Stateful In-Memory + Persistent Dedup Index

The dedup index is a `defaultdict[str, list[dict]]` loaded from a per-account
CSV at the start of each run. New rows are appended to the in-memory dict
during the loop so that intra-batch duplicates are caught before the index is
committed.

The index filename is `<partition_value>-duplicate-index.csv` (e.g.
`BE12345678901234-duplicate-index.csv`), derived from
`duplicate_key.partition_by` in the YAML (see pattern 3). If `partition_by`
is absent, it falls back to `<bank>-duplicate-index.csv`.

`classify_duplicate()` in [engine/core/duplicate_index.py](../../engine/core/duplicate_index.py)
returns:
- `new` — key not seen.
- `identical` — key seen, all required fields match → silently skip.
- `conflict` — key seen, required fields differ → write to duplicate-failed.

The index is committed atomically at the end: snapshot (timestamped copy in
`backups/`) → copy-to-live → rotate backups (`rotate_duplicate_backups`,
capped at `MAX_BACKUPS=50` and `MAX_BACKUP_AGE_DAYS=365`). A rollback copy
(`previous-duplicate-index.csv` in `data/temp/`) allows recovery if the
normalized-output move fails — see step 4 of `finalize`.

## 8. Tiered Error Criticality

The step-numbered blocks inside `finalize()` in
[engine/core/completion.py](../../engine/core/completion.py) distinguish:

- **Non-critical** (wrap in try/except, log, continue): duplicate-index
  prep, backup rotation, temp cleanup, logging itself.
- **Critical** (attempt compensating move, email, exit): original CSV move
  (exit 94), duplicate-index commit (93), normalized output move (92),
  duplicate-index prep (97).
- **Catastrophic** (exit 99): unhandled exception anywhere in the pipeline.

Non-critical failures never interrupt the happy path. Critical failures
always attempt a compensating action (move processed file to failed,
roll back index from `previous-duplicate-index.csv`) before exiting.

## 9. Context Dict as Pipeline State

A single `context` dict is threaded through all phases and into `finalize()`.
It carries `paths`, `open_writers`, `duplicate_index_rows_to_add`,
`log_event`, and the run timestamp. This avoids module-global state while
keeping the pipeline callable as a unit — see `main()` in
[engine/process_csv.py](../../engine/process_csv.py).

## 10. Timestamp-Everything Convention

All normalized output, processed originals, failed rows, logs, and
duplicate-index backups include the run timestamp `YYYYMMDD-HHMMSS` in the
filename. This makes concurrent runs distinguishable and provides a
complete audit trail without a database.

Format constant `RUN_TS_FORMAT` is defined in
[engine/core/duplicate_index.py](../../engine/core/duplicate_index.py); generated by
[bank-csv-normalizer.bash](../../bank-csv-normalizer.bash) (`date '+%Y%m%d-%H%M%S'`)
and consumed by `build_paths` in [engine/core/csv_runtime.py](../../engine/core/csv_runtime.py).

## 11. Cron-Safe Orchestration

[bank-csv-normalizer.bash](../../bank-csv-normalizer.bash) guards unattended execution:

- Sets `PYTHONPATH` and `cd`s into `BASE_DIR` (cron has no defaults).
- Single lockfile (`.process.lock`) with `trap cleanup INT TERM EXIT` so
  crashed runs cannot leave the lock behind.
- **Size-stability check** — `stat` twice with a 2s sleep; skip if the file
  is still growing (defends against partial SFTP uploads).
- Exit codes `0/65/75/99` are "Python handled it"; anything else triggers a
  fallback move of the incoming file to `data/failed/`.

## 12. Naming Conventions

All code uses explicit, descriptive English names. Related values share
prefixes to make the origin obvious in reconciliation blocks —
`column_*` for values pulled from dedicated CSV columns, `details_*` for
values extracted from the free-text `details` column, `normalized_*` for
final output fields. Names reflect meaning and purpose; no abbreviations
except common ones (IBAN, BIC, CSV).
