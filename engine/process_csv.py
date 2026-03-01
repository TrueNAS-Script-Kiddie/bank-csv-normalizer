#!/usr/bin/env python3
import sys
import os
import csv
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from engine.core.csv_runtime import (
    load_csv_rows,
    write_failed_row,
    load_normalized_rows,
    build_paths,
    ensure_writer,
    load_all_bank_configs,
)
from engine.core.csv_validation import (
    autodetect_bank,
    validate_and_prepare,
    extract_duplicate_key,
)

from engine.core.normalize import normalize_row
from engine.core.duplicate_index import load_duplicate_index, classify_duplicate
from engine.core.runtime import log_event
import engine.core.completion as completion


# -------------------------------------------------------------------------
# Directory configuration
# -------------------------------------------------------------------------
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR: str = os.path.join(BASE_DIR, "data")
CONFIG_DIR: str = os.path.join(BASE_DIR, "config")

# Globals set in main()
csv_file_path: str
csv_filename: str
run_timestamp: str
logfile_path: str


# -------------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------------
def main() -> None:
    global csv_file_path, csv_filename, run_timestamp, logfile_path

    if len(sys.argv) != 4:
        print("Usage: process_csv.py <csv_path> <run_timestamp> <logfile_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    run_timestamp = sys.argv[2]
    logfile_path = sys.argv[3]
    csv_filename = os.path.basename(csv_file_path)

    # ---------------------------------------------------------------------
    # BUILD PATHS → canonical paths[] dict
    # ---------------------------------------------------------------------
    paths = build_paths(
        data_dir=DATA_DIR,
        run_timestamp=run_timestamp,
        csv_filename=csv_filename,
    )

    # ---------------------------------------------------------------------
    # CONTEXT (legacy + new paths[])
    # ---------------------------------------------------------------------
    context: Dict[str, Any] = {
        "csv_file_path": csv_file_path,
        "csv_filename": csv_filename,
        "run_timestamp": run_timestamp,
        "logfile_path": logfile_path,
        "paths": paths,
        "open_writers": [],
        "log_event": log_event,
    }

    try:
        # Ensure directory structure exists
        for d in (
            paths["processed_dir"],
            paths["failed_dir"],
            paths["normalized_dir"],
            paths["duplicate_index_dir"],
            paths["duplicate_index_backup_dir"],
            paths["temp_dir"],
        ):
            os.makedirs(d, exist_ok=True)

        # -----------------------------------------------------------------
        # Load all bank configs
        # -----------------------------------------------------------------
        bank_configs = load_all_bank_configs(CONFIG_DIR)

        # -----------------------------------------------------------------
        # Load CSV
        # -----------------------------------------------------------------
        csv_rows: List[Dict[str, Any]] = load_csv_rows(csv_file_path)
        log_event(logfile_path, f"Loaded {len(csv_rows)} raw rows")

        if not csv_rows:
            completion.finalize(
                context,
                exit_code=65,
                outcome="structure_failed",
                normalized_rows=None,
                message="CSV EMPTY",
            )
            return

        # -----------------------------------------------------------------
        # Autodetect bank
        # -----------------------------------------------------------------
        bank_cfg = autodetect_bank(csv_rows, bank_configs)
        if bank_cfg is None:
            completion.finalize(
                context,
                exit_code=65,
                outcome="structure_failed",
                normalized_rows=None,
                message="BANK AUTODETECT FAILED",
            )
            return

        log_event(logfile_path, f"Detected bank: {bank_cfg['bank']}")

        # -----------------------------------------------------------------
        # Set bank-specific duplicate-index path
        # -----------------------------------------------------------------
        paths["duplicate_index_csv"] = os.path.join(
            paths["duplicate_index_dir"],
            f"{bank_cfg['bank']}-duplicate-index.csv",
        )

        # -----------------------------------------------------------------
        # Validate + map + filter rows
        # -----------------------------------------------------------------
        validated_rows, column_map = validate_and_prepare(csv_rows, bank_cfg)
        log_event(logfile_path, f"Validated {len(validated_rows)} rows after filtering")

        if not validated_rows:
            completion.finalize(
                context,
                exit_code=65,
                outcome="structure_failed",
                normalized_rows=None,
                message="NO VALID ROWS AFTER VALIDATION",
            )
            return

        # -----------------------------------------------------------------
        # Load duplicate index (bank-specific)
        # -----------------------------------------------------------------
        duplicate_index = load_duplicate_index(paths["duplicate_index_csv"])
        log_event(
            logfile_path,
            f"Loaded duplicate index with {sum(len(v) for v in duplicate_index.values())} rows",
        )

        # -----------------------------------------------------------------
        # Initialize pipeline state
        # -----------------------------------------------------------------
        failed_any = False
        normalized_any = False

        # Rows that must be added to the duplicate-index
        duplicate_index_rows_to_add: List[Dict[str, Any]] = []
        context["duplicate_index_rows_to_add"] = duplicate_index_rows_to_add

        # Required fields for duplicate-index rows
        duplicate_index_required_fields = list(bank_cfg["columns"]["required"].keys())

        # -----------------------------------------------------------------
        # Initialize writers
        # -----------------------------------------------------------------
        normalize_failed_ref = {"writer": None, "file": None}
        duplicate_failed_ref = {"writer": None, "file": None}
        temp_normalized_ref = {"writer": None, "file": None}

        context["open_writers"] = [
            temp_normalized_ref,
            normalize_failed_ref,
            duplicate_failed_ref,
        ]

        # -----------------------------------------------------------------
        # Row processing loop
        # -----------------------------------------------------------------
        for row in validated_rows:

            # Extract duplicate key
            key = extract_duplicate_key(row, bank_cfg)
            if not key:
                failed_any = True
                write_failed_row(paths["failed_duplicate_csv"], duplicate_failed_ref, row)
                log_event(logfile_path, f"Missing duplicate_key for row: {row}")
                continue

            # Duplicate check
            status = classify_duplicate(duplicate_index, key, row, bank_cfg)

            if status == "identical":
                log_event(logfile_path, f"IGNORED identical row with key {key}")
                continue

            if status == "conflict":
                failed_any = True
                write_failed_row(paths["failed_duplicate_csv"], duplicate_failed_ref, row)
                log_event(logfile_path, f"DUPLICATE key (non-identical) {key}")
                continue

            # NEW ROW → add to duplicate-index list
            duplicate_index_row = {"duplicate_key": key}
            for field in duplicate_index_required_fields:
                duplicate_index_row[field] = row.get(field, "")
            duplicate_index_rows_to_add.append(duplicate_index_row)

            # Update in-memory duplicate index so later rows see this one
            duplicate_index.setdefault(key, []).append(duplicate_index_row)

            # Normalize row
            try:
                normalized_row = normalize_row(row)
            except Exception:
                failed_any = True
                write_failed_row(paths["failed_normalize_csv"], normalize_failed_ref, row)
                log_event(logfile_path, "Normalize failed for a row")
                continue

            # Valid normalized row
            normalized_any = True
            temp_output_writer = ensure_writer(
                paths["temp_normalized_csv"],
                temp_normalized_ref,
                list(normalized_row.keys()),
            )
            temp_output_writer.writerow(normalized_row)

        # -----------------------------------------------------------------
        # Final classification
        # -----------------------------------------------------------------
        if failed_any and not normalized_any:
            completion.finalize(
                context,
                exit_code=65,
                outcome="all_failed",
                normalized_rows=None,
                message="CSV ALL FAILED",
            )
            return

        if failed_any and normalized_any:
            completion.finalize(
                context,
                exit_code=75,
                outcome="partial",
                normalized_rows=load_normalized_rows(paths["temp_normalized_csv"]),
                message="CSV PARTIAL SUCCESS",
            )
            return

        if not failed_any and normalized_any:
            completion.finalize(
                context,
                exit_code=0,
                outcome="success",
                normalized_rows=load_normalized_rows(paths["temp_normalized_csv"]),
                message="CSV SUCCESS",
            )
            return

        if not failed_any and not normalized_any:
            completion.finalize(
                context,
                exit_code=0,
                outcome="all_full_duplicates",
                normalized_rows=None,
                message="CSV ALL FULL DUPLICATES",
            )

    except Exception as e:
        completion.finalize(
            context,
            exit_code=99,
            outcome="error",
            normalized_rows=None,
            message=f"UNEXPECTED ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )


if __name__ == "__main__":
    main()