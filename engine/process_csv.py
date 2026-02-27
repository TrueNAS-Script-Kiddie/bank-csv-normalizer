#!/usr/bin/env python3
import sys
import os
import csv
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.csv_runtime import (
    load_csv_rows,
    validate_csv_structure,
    extract_key,
    write_failed_row,
    load_normalized_rows,
    build_paths,
    ensure_writer,
)

from engine.core.normalize import normalize_row
from core.duplicate_index import load_duplicate_index, classify_duplicate
from engine.core.runtime import log_event
import core.completion as completion


# -------------------------------------------------------------------------
# Directory configuration
# -------------------------------------------------------------------------
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR: str = os.path.join(BASE_DIR, "data")

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
        "paths": paths,               # canonical
        "open_writers": [],
        "log_event": log_event,
    }

    # Global try MUST start as early as possible
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

        # Load CSV
        csv_rows: List[Dict[str, Any]] = load_csv_rows(csv_file_path)
        log_event(logfile_path, f"Loaded {len(csv_rows)} rows")

        # Validate structure
        if not validate_csv_structure(csv_rows):
            completion.finalize(
                context,
                exit_code=65,
                outcome="structure_failed",
                normalized_rows=None,
                message="CSV STRUCTURE FAILED",
            )
            return

        log_event(logfile_path, "CSV structure/content check PASSED")

        # Load duplicate index
        duplicate_index: Dict[str, List[Dict[str, Any]]] = load_duplicate_index(paths["duplicate_index_csv"])
        log_event(
            logfile_path,
            f"Loaded duplicate index with {sum(len(v) for v in duplicate_index.values())} rows",
        )

        failed_any: bool = False
        normalized_any: bool = False

        # Writers
        normalize_failed_ref: Dict[str, Any] = {"writer": None, "file": None}
        duplicate_failed_ref: Dict[str, Any] = {"writer": None, "file": None}
        temp_normalized_ref: Dict[str, Any] = {"writer": None, "file": None}

        context["open_writers"] = [
            temp_normalized_ref,
            normalize_failed_ref,
            duplicate_failed_ref,
        ]

        # -----------------------------------------------------------------
        # Row processing loop
        # -----------------------------------------------------------------
        for csv_row in csv_rows:
            # Normalize single row
            try:
                normalized_row: Dict[str, Any] = normalize_row(csv_row)
            except Exception:
                failed_any = True
                write_failed_row(paths["failed_normalize_csv"], normalize_failed_ref, csv_row)
                log_event(logfile_path, "Normalize failed for a row")
                continue

            key: Optional[str] = extract_key(normalized_row)
            if not key:
                failed_any = True
                write_failed_row(paths["failed_normalize_csv"], normalize_failed_ref, normalized_row)
                log_event(logfile_path, "Missing BANKREFERENTIE for a row")
                continue

            # Duplicate check
            status = classify_duplicate(duplicate_index, key, normalized_row)

            if status == "identical":
                log_event(logfile_path, f"IGNORED identical row with key {key}")
                continue

            if status == "conflict":
                failed_any = True
                write_failed_row(paths["failed_duplicate_csv"], duplicate_failed_ref, normalized_row)
                log_event(logfile_path, f"DUPLICATE key (non-identical) {key}")
                continue

            # Valid normalized row
            normalized_any = True
            temp_output_writer = ensure_writer(
                paths["temp_normalized_csv"],
                temp_normalized_ref,
                list(normalized_row.keys()),
            )
            temp_output_writer.writerow(normalized_row)

        # Final classification:
        # - Key-duplicates count as failures
        # - Full-duplicates are ignored unless every row is a full-duplicate (all_full_duplicates)
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