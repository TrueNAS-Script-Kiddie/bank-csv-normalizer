# Architectural Patterns

## 1. Linear Multi-Phase Pipeline

The pipeline in [engine/process_csv.py](../../engine/process_csv.py) runs a strict,
ordered sequence. Each phase must succeed before the next begins:

1. **Load** — encoding/delimiter auto-detection ([csv_runtime.py:74](../../engine/core/csv_runtime.py#L74))
2. **Autodetect bank** — column-header matching against all YAML configs ([csv_validation.py:164](../../engine/core/csv_validation.py#L164))
3. **Validate & map** — column remapping, filter rules, regex checks ([csv_validation.py:7](../../engine/core/csv_validation.py#L7))
4. **Load dedup index** — bank-specific persistent CSV ([duplicate_index.py:31](../../engine/core/duplicate_index.py#L31))
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

New banks require only a new YAML file. See [config/fintro.yaml](../../config/fintro.yaml).

## 4. Lazy Writer Pattern

CSV writers are never opened speculatively. Each output file (temp-normalized,
normalize-failed, duplicate-failed) uses a `{"writer": None, "file": None}` ref
dict. `ensure_writer()` creates the file+writer on first write, inserting the
header once.

All refs are collected in `context["open_writers"]` for guaranteed cleanup in
`completion.close_open_writers()`.

See [csv_runtime.py:137](../../engine/core/csv_runtime.py#L137) and [process_csv.py:203](../../engine/process_csv.py#L203).

## 5. Two-Source Data Reconciliation

For every field that appears in both CSV columns and the free-text `details_raw`
column, the normalizer compares both sources and raises `ValueError` on mismatch.
Neither source is blindly trusted.

Precedence rules in [normalize.py:183](../../engine/core/normalize.py#L183):
- **CSV wins** for amounts, IBANs, dates, and free-text messages (details validates)
- **Details wins** for structured references, BIC, and fields missing from CSV
- **Structured messages** (Belgian `+++xxx/xxxx/xxxxx+++` format) take priority
  over free-text when either source has one

## 6. Sequential Regex Parsing of details_raw

`normalize_row()` works through the `details_raw` string destructively:
each matched segment is stripped from `details_rest` after extraction.
Steps are labeled B1–B11. If anything remains in `details_rest` at phase C,
it raises `ValueError("Unprocessed details content: ...")`.

This makes the parser strict by default — unknown content is an error, not
silently ignored. See [normalize.py:252](../../engine/core/normalize.py#L252)–[normalize.py:582](../../engine/core/normalize.py#L582).

All regex patterns are module-level compiled constants (`RE_*`) at the top of
[normalize.py:10](../../engine/core/normalize.py#L10).

## 7. Stateful In-Memory + Persistent Dedup Index

The dedup index is a `DefaultDict[str, List[Dict]]` loaded from a bank-specific
CSV at the start of each run. New rows are appended to the in-memory dict during
the loop so that intra-batch duplicates are caught before the index is committed.

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
