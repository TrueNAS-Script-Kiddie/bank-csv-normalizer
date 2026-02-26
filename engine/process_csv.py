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
    ensure_writer,
    load_transformed_rows,
)

from engine.core.transform import transform_row
from core.duplicate_index import load_duplicate_index
import core.completion as completion


# -------------------------------------------------------------------------
# Directory configuration
# -------------------------------------------------------------------------
BASE_DIR: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR: str = os.path.join(BASE_DIR, "data")

PROCESSED_DIR: str = os.path.join(DATA_DIR, "processed")
FAILED_DIR: str = os.path.join(DATA_DIR, "failed")
NORMALIZED_DIR: str = os.path.join(DATA_DIR, "normalized")
TEMP_DIR: str = os.path.join(DATA_DIR, "temp")
DUPLICATE_INDEX_DIR: str = os.path.join(DATA_DIR, "duplicate-index")
BACKUP_DIR: str = os.path.join(DUPLICATE_INDEX_DIR, "backups")

DUPLICATE_INDEX_PATH: str = os.path.join(DUPLICATE_INDEX_DIR, "duplicate-index.csv")


# Globals set in main()
csv_file_path: str
csv_filename: str
run_timestamp: str
logfile_path: str
temp_output_path: Optional[str] = None


# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
def log_event(logfile_path: str, message: str) -> None:
    """Append a timestamped log message to the logfile."""
    timestamp: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} {message}\n")


# -------------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------------
def main() -> None:
    global csv_file_path, csv_filename, run_timestamp, logfile_path, temp_output_path

    if len(sys.argv) != 4:
        print("Usage: process_csv.py <csv_path> <run_timestamp> <logfile_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    run_timestamp = sys.argv[2]
    logfile_path = sys.argv[3]
    csv_filename = os.path.basename(csv_file_path)

    # Expose globals to completion.py
    completion.csv_file_path = csv_file_path
    completion.csv_filename = csv_filename
    completion.run_timestamp = run_timestamp
    completion.logfile_path = logfile_path

    completion.PROCESSED_DIR = PROCESSED_DIR
    completion.NORMALIZED_DIR = NORMALIZED_DIR
    completion.FAILED_DIR = FAILED_DIR
    completion.TEMP_DIR = TEMP_DIR
    completion.DUPLICATE_INDEX_PATH = DUPLICATE_INDEX_PATH
    completion.BACKUP_DIR = BACKUP_DIR

    try:
        # Ensure directory structure exists
        for d in (
            PROCESSED_DIR,
            FAILED_DIR,
            NORMALIZED_DIR,
            DUPLICATE_INDEX_DIR,
            BACKUP_DIR,
            TEMP_DIR,
        ):
            os.makedirs(d, exist_ok=True)

        # Load CSV
        csv_rows: List[Dict[str, Any]] = load_csv_rows(csv_file_path)
        log_event(logfile_path, f"Loaded {len(csv_rows)} rows")

        # Validate structure
        if not validate_csv_structure(csv_rows):
            completion.finalize(
                exit_code=65,
                move_suffix="-failed.csv",
                normalized_suffix=None,
                transformed_rows=None,
                message="CSV STRUCTURE FAILED",
            )
            return

        log_event(logfile_path, "CSV structure/content check PASSED")

        # Load duplicate index
        duplicate_index: Dict[str, List[Dict[str, Any]]] = load_duplicate_index(DUPLICATE_INDEX_PATH)
        log_event(
            logfile_path,
            f"Loaded duplicate index with {sum(len(v) for v in duplicate_index.values())} rows",
        )

        failed_any: bool = False
        transformed_any: bool = False

        # Prepare temp output writer
        temp_output_path = os.path.join(TEMP_DIR, f"{run_timestamp}-{csv_filename}.tmp.csv")
        temp_output_file = open(temp_output_path, "w", newline="", encoding="utf-8")
        temp_output_writer: Optional[csv.DictWriter] = None

        # Register writer for final cleanup
        completion.open_writers.append({"file": temp_output_file})

        # Writers for failed rows
        transform_failed_ref: Dict[str, Any] = {"writer": None, "file": None}
        duplicate_failed_ref: Dict[str, Any] = {"writer": None, "file": None}

        completion.open_writers.append(transform_failed_ref)
        completion.open_writers.append(duplicate_failed_ref)

        transform_failed_path: str = os.path.join(
            FAILED_DIR,
            f"{run_timestamp}-{csv_filename}-transform-failed_rows.csv",
        )
        duplicate_failed_path: str = os.path.join(
            FAILED_DIR,
            f"{run_timestamp}-{csv_filename}-duplicate_rows.csv",
        )

        # Row processing loop
        for csv_row in csv_rows:
            # Transform
            try:
                transformed_row: Dict[str, Any] = transform_row(csv_row)
            except Exception:
                failed_any = True
                w = ensure_writer(transform_failed_path, transform_failed_ref, list(csv_row.keys()))
                w.writerow(csv_row)
                log_event(logfile_path, "Transform failed for a row")
                continue

            key: Optional[str] = extract_key(transformed_row)
            if not key:
                failed_any = True
                w = ensure_writer(transform_failed_path, transform_failed_ref, list(csv_row.keys()))
                w.writerow(csv_row)
                log_event(logfile_path, "Missing BANKREFERENTIE for a row")
                continue

            # Duplicate check
            existing_rows: List[Dict[str, Any]] = duplicate_index.get(key, [])
            if any(r == transformed_row for r in existing_rows):
                log_event(logfile_path, f"IGNORED identical row with key {key}")
                continue

            if existing_rows:
                failed_any = True
                w = ensure_writer(duplicate_failed_path, duplicate_failed_ref, list(transformed_row.keys()))
                w.writerow(transformed_row)
                log_event(logfile_path, f"DUPLICATE key (non-identical) {key}")
                continue

            # Valid transformed row
            transformed_any = True
            if temp_output_writer is None:
                temp_output_writer = csv.DictWriter(
                    temp_output_file,
                    fieldnames=list(transformed_row.keys()),
                )
                temp_output_writer.writeheader()
            temp_output_writer.writerow(transformed_row)

            duplicate_index.setdefault(key, []).append(transformed_row)

        # Final classification
        if failed_any and not transformed_any:
            completion.finalize(
                exit_code=65,
                move_suffix="-failed.csv",
                normalized_suffix=None,
                transformed_rows=None,
                message="CSV FAILED (ALL ROWS)",
            )
            return

        if failed_any and transformed_any:
            transformed_rows = load_transformed_rows(temp_output_path)
            completion.finalize(
                exit_code=75,
                move_suffix="-partial.csv",
                normalized_suffix="-partial.csv",
                transformed_rows=transformed_rows,
                message="CSV PARTIAL SUCCESS",
            )
            return

        if not failed_any and transformed_any:
            transformed_rows = load_transformed_rows(temp_output_path)
            completion.finalize(
                exit_code=0,
                move_suffix=".csv",
                normalized_suffix=".csv",
                transformed_rows=transformed_rows,
                message="CSV SUCCESS",
            )
            return

        # All duplicates
        completion.finalize(
            exit_code=0,
            move_suffix=".csv",
            normalized_suffix=None,
            transformed_rows=None,
            message="CSV FULL DUPLICATES",
        )

    except Exception as e:
        completion.finalize(
            exit_code=99,
            move_suffix="-failed.csv",
            normalized_suffix=None,
            transformed_rows=None,
            message=f"UNEXPECTED ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )


if __name__ == "__main__":
    main()
