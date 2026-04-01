# Architectural Patterns

## 1. Linear Multi-Phase Pipeline

The pipeline in [engine/process_csv.py](../../engine/process_csv.py) runs a strict,
ordered sequence. Each phase must succeed before the next begins:

1. **Load** — encoding/delimiter auto-detection ([csv_runtime.py:74](../../engine/core/csv_runtime.py#L74))
2. **Autodetect bank** — column-header matching against all YAML configs ([csv_validation.py:164](../../engine/core/csv_validation.py#L164))
3. **Validate & map** — column remapping, filter rules, regex checks ([csv_validation.py:7](../../engine/core/csv_validation.py#L7))
4. **Load dedup index** — account-specific persistent CSV ([duplicate_index.py:31](../../engine/core/duplicate_index.py#L31))
5. **Per-row loop** — dedup check → normalize → write temp output ([process_csv.py:216](../../engine/process_csv.py#L216))
6. **Outcome classification** — success / partial / all_failed / all_full_duplicates ([process_csv.py:278](../../engine/process_csv.py#L278))
7. **Finalize** — single exit path; all file moves, index commit, email ([completion.py:74](../../engine/core/completion.py#L74))

## 2. Single Exit Path (completion.finalize)

`completion.finalize()` is the **only** place that exits the Python process.
Every code path (normal, error, structure failure) calls it. This guarantees
that file moves, index commits, writer cleanup, and email always happen together.

See [completion.py:74](../../engine/core/completion.py#L74). Outcomes map 1:1 to exit codes and
destination subdirectories.

## 3. Configuration-Driven Bank Support

All bank-specific behavior lives in `config/<bank>.yaml`. The engine has zero
hardcoded bank names. Key YAML sections:

- `columns.required` — expected CSV headers, with `names` (aliases), `regex`
  (per-cell validation), and `filter` (row inclusion rules)
- `columns.optional` — present-if-exists headers
- `duplicate_key` — which column(s) to extract the dedup key from, with an
  optional regex to pull a sub-value

The bank name is derived from the YAML filename by `load_all_bank_configs()` and
injected as `cfg["bank"]` ([csv_runtime.py:192](../../engine/core/csv_runtime.py#L192)).

**`engine/banks/<bank>.py`** — normalization logic, must export `normalize_row(csv_row)`.
Loaded dynamically after detection: `importlib.import_module(f"engine.banks.{bank_name}")` ([process_csv.py:146](../../engine/process_csv.py#L146)).

Adding a new bank requires only `config/<bank>.yaml` + `engine/banks/<bank>.py`.
See [config/fintro.yaml](../../config/fintro.yaml) and [engine/banks/fintro.py](../../engine/banks/fintro.py).

## 4. Lazy Writer Pattern

CSV writers are never opened speculatively. Each output file (temp-normalized,
normalize-failed, duplicate-failed) uses a `{"writer": None, "file": None}` ref
dict. `ensure_writer()` creates the file+writer on first write, inserting the
header once.

All refs are collected in `context["open_writers"]` for guaranteed cleanup in
`completion.close_open_writers()`.

See [csv_runtime.py:137](../../engine/core/csv_runtime.py#L137) and [process_csv.py:203](../../engine/process_csv.py#L203).

## 5. Two-Source Data Reconciliation

For every field that appears in both a dedicated column and multi-purpose `details` 
column, the normalizer compares both sources and raises `ValueError` on mismatch or appends both values to the normalized row.
Neither source is blindly trusted.

Precedence rules in [engine/banks/fintro.py:188](../../engine/banks/fintro.py#L188):
- **CSV wins** for amounts, IBANs, dates, and free-text messages (details validates)
- **Details wins** for structured references, BIC, and fields missing from CSV
- **Structured messages** (Belgian `+++xxx/xxxx/xxxxx+++` format) take priority
  over free-text when either source has one

## 6. Two-Phase normalize_row + Sequential details Parsing

`normalize_row()` in [engine/banks/fintro.py](../../engine/banks/fintro.py) is split into two explicit phases:

**Phase 1 — Extraction.** All values are pulled into named variables; no output is written yet.
- *Pre-A steps*: individual dedicated columns are extracted from `csv_row` by key.
- *A steps*: everything is extracted from the multi-purpose `details` column in two sub-passes:
  1. Common, easy-to-detect postfixes (anchored with `$`) are stripped first: VALUTADATUM, BANKREFERENTIE, UITGEVOERD OP.
  2. The remainder is matched by leading pattern (anchored with `^`) to identify the transaction type and extract all remaining fields.
  Each matched segment is removed from `remaining_details`. Anything left at the end raises `ValueError`.

**Phase 2 — Write.** Extracted variables are reconciled across sources and written to the `normalized` dict.
No regex or string parsing; only decisions (cross-source comparison, suppression, assembly).

## 7. Stateful In-Memory + Persistent Dedup Index

The dedup index is a `defaultdict[str, list[dict]]` loaded from a per-account
CSV at the start of each run. New rows are appended to the in-memory dict during
the loop so that intra-batch duplicates are caught before the index is committed.

The index file is named `<IBAN>-duplicate-index.csv` (e.g. `BE12345678901234-duplicate-index.csv`).
Which column provides the partition value is configured in the YAML:
```yaml
duplicate_key:
  columns: ["external_id"]
  partition_by: "asset_account_iban"
```
If `partition_by` is absent, the index falls back to one file per bank name.

Classification ([duplicate_index.py:173](../../engine/core/duplicate_index.py#L173)):
- `new` — key not seen
- `identical` — key seen, all required fields match → silently skip
- `conflict` — key seen, required fields differ → write to duplicate-failed

The index is committed atomically at the end: snapshot → copy-to-live → rotate
backups. A rollback copy (`previous-duplicate-index.csv`) allows recovery if the
normalized-output move fails. See [completion.py:148](../../engine/core/completion.py#L148).

## 8. Tiered Error Criticality

Not all errors are equal. The finalization sequence in [completion.py](../../engine/core/completion.py)
distinguishes:

- **Non-critical** (wrap in try/except, log, continue): duplicate-index prep,
  backup rotation, temp cleanup
- **Critical** (move file to failed, email, exit): original CSV move (94),
  index commit (93), normalized output move (92)
- **Catastrophic** (exit 99): unhandled exception anywhere in the pipeline

Non-critical failures never interrupt the happy path. Critical failures always
attempt a compensating action (e.g. move processed file back to failed) before
exiting.

## 9. Context Dict as Pipeline State

A single `context` dict is threaded through all phases and into `finalize()`.
It carries paths, open writers, rows to add to the dedup index, log handles, etc.
This avoids global state while keeping the pipeline callable as a unit.

See [process_csv.py:87](../../engine/process_csv.py#L87).

## 10. Timestamp-Everything Convention

All output files, log files, and dedup-index backups include the run timestamp
(`YYYYMMDD-HHMMSS`) in the filename. This makes concurrent runs distinguishable
and provides a complete audit trail without a database.

Format defined at [duplicate_index.py:25](../../engine/core/duplicate_index.py#L25); applied in
[csv_runtime.py:54](../../engine/core/csv_runtime.py#L54) and [bank-csv-normalizer.bash:34](../../bank-csv-normalizer.bash#L34).

## 11. Naming Conventions

All code uses clear, explicit, and consistent English names.

- Use descriptive names for variables, functions, modules, and files.
- Apply consistent naming patterns for related values (e.g., shared prefixes for CSV-derived fields).
- Names must reflect meaning and purpose.