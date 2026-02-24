#!/usr/bin/env python3
import sys
import os
import csv
import shutil
import traceback
from datetime import datetime

# Local modules
from core.csv_model import (
    load_csv_rows,
    validate_csv_structure,
    extract_key,
)
from core.transform_logic import transform_row
from core.duplicate_index import (
    load_duplicate_index,
    append_to_duplicate_index,
)

# -------------------------------------------------------------------------
# Directory configuration (module-level constants)
# -------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

INCOMING_DIR = os.path.join(DATA_DIR, "incoming")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
FAILED_DIR = os.path.join(DATA_DIR, "failed")
NORMALIZED_DIR = os.path.join(DATA_DIR, "normalized")
TEMP_DIR = os.path.join(DATA_DIR, "temp")
DUPLICATE_INDEX_DIR = os.path.join(DATA_DIR, "duplicate-index")

DUPLICATE_INDEX_PATH = os.path.join(DUPLICATE_INDEX_DIR, "duplicate-index.csv")


# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
def log_event(logfile_path: str, message: str) -> None:
    """Append timestamped log entry."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{timestamp} {message}\n")


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    # ---------------------------------------------------------------------
    # Arguments
    # ---------------------------------------------------------------------
    if len(sys.argv) != 4:
        print("Usage: process_csv.py <csv_path> <run_timestamp> <logfile_path>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    run_timestamp = sys.argv[2]
    logfile_path = sys.argv[3]
    csv_filename = os.path.basename(csv_file_path)

    # ---------------------------------------------------------------------
    # Ensure required directories exist
    # ---------------------------------------------------------------------
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)
    os.makedirs(NORMALIZED_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(DUPLICATE_INDEX_DIR, exist_ok=True)

    # ---------------------------------------------------------------------
    # Load CSV
    # ---------------------------------------------------------------------
    try:
        csv_rows = load_csv_rows(csv_file_path)
        log_event(logfile_path, f"Loaded {len(csv_rows)} rows")
    except Exception as exception:
        log_event(logfile_path, f"ERROR loading CSV: {exception}")
        traceback_str = traceback.format_exc()
        log_event(logfile_path, f"TRACEBACK:\n{traceback_str}")
        sys.exit(1)

    # ---------------------------------------------------------------------
    # Validate structure
    # ---------------------------------------------------------------------
    if not validate_csv_structure(csv_rows):
        log_event(logfile_path, "CSV structure/content check FAILED")

        processed_failed_path = os.path.join(
            PROCESSED_DIR,
            f"{run_timestamp}-{csv_filename}-failed.csv",
        )
        shutil.move(csv_file_path, processed_failed_path)

        sys.exit(100)

    log_event(logfile_path, "CSV structure/content check PASSED")

    # ---------------------------------------------------------------------
    # Load duplicate index
    # ---------------------------------------------------------------------
    duplicate_index = load_duplicate_index(DUPLICATE_INDEX_PATH)
    log_event(
        logfile_path,
        f"Loaded duplicate index with {sum(len(rows) for rows in duplicate_index.values())} rows",
    )

    failed_any = False
    transformed_any = False

    # ---------------------------------------------------------------------
    # Prepare output files (temp + failed)
    # ---------------------------------------------------------------------
    temp_output_path = os.path.join(
        TEMP_DIR,
        f"{run_timestamp}-{csv_filename}.tmp.csv",
    )
    temp_output_file = open(temp_output_path, "w", newline="", encoding="utf-8")
    temp_output_writer = None

    transform_failed_path = os.path.join(
        FAILED_DIR,
        f"{run_timestamp}-{csv_filename}-transform-failed_rows.csv",
    )
    duplicate_failed_path = os.path.join(
        FAILED_DIR,
        f"{run_timestamp}-{csv_filename}-duplicate_rows.csv",
    )

    transform_failed_file = None
    transform_failed_writer = None
    duplicate_failed_file = None
    duplicate_failed_writer = None

    # ---------------------------------------------------------------------
    # Row-by-row processing
    # ---------------------------------------------------------------------
    try:
        for csv_row in csv_rows:

            # Transform
            try:
                transformed_row = transform_row(csv_row)
            except Exception:
                failed_any = True
                if transform_failed_writer is None:
                    transform_failed_file = open(
                        transform_failed_path,
                        "w",
                        newline="",
                        encoding="utf-8",
                    )
                    transform_failed_writer = csv.DictWriter(
                        transform_failed_file,
                        fieldnames=list(csv_row.keys()),
                    )
                    transform_failed_writer.writeheader()
                transform_failed_writer.writerow(csv_row)
                log_event(logfile_path, "Transform failed for a row")
                continue

            key = extract_key(transformed_row)
            if not key:
                failed_any = True
                if transform_failed_writer is None:
                    transform_failed_file = open(
                        transform_failed_path,
                        "w",
                        newline="",
                        encoding="utf-8",
                    )
                    transform_failed_writer = csv.DictWriter(
                        transform_failed_file,
                        fieldnames=list(csv_row.keys()),
                    )
                    transform_failed_writer.writeheader()
                transform_failed_writer.writerow(csv_row)
                log_event(logfile_path, "Missing BANKREFERENTIE for a row")
                continue

            # Duplicate check
            existing_rows = duplicate_index.get(key, [])
            is_identical = any(existing_row == transformed_row for existing_row in existing_rows)

            if is_identical:
                log_event(logfile_path, f"IGNORED identical row with key {key}")
                continue

            if existing_rows:
                failed_any = True
                if duplicate_failed_writer is None:
                    duplicate_failed_file = open(
                        duplicate_failed_path,
                        "w",
                        newline="",
                        encoding="utf-8",
                    )
                    duplicate_failed_writer = csv.DictWriter(
                        duplicate_failed_file,
                        fieldnames=list(transformed_row.keys()),
                    )
                    duplicate_failed_writer.writeheader()
                duplicate_failed_writer.writerow(transformed_row)
                log_event(logfile_path, f"DUPLICATE key (non-identical) {key}")
                continue

            # Not a duplicate → transformed
            transformed_any = True
            if temp_output_writer is None:
                temp_output_writer = csv.DictWriter(
                    temp_output_file,
                    fieldnames=list(transformed_row.keys()),
                )
                temp_output_writer.writeheader()
            temp_output_writer.writerow(transformed_row)

            duplicate_index[key].append(transformed_row)

    except Exception as exception:
        log_event(logfile_path, f"ERROR during row processing: {exception}")
        traceback_str = traceback.format_exc()
        log_event(logfile_path, f"TRACEBACK:\n{traceback_str}")
        sys.exit(1)

    finally:
        temp_output_file.close()
        if transform_failed_file:
            transform_failed_file.close()
        if duplicate_failed_file:
            duplicate_failed_file.close()

    # ---------------------------------------------------------------------
    # Final classification
    # ---------------------------------------------------------------------
    temp_output_exists = os.path.exists(temp_output_path)

    # A) All failed
    if failed_any and not transformed_any:
        log_event(logfile_path, "All rows failed")

        processed_failed_path = os.path.join(
            PROCESSED_DIR,
            f"{run_timestamp}-{csv_filename}-failed.csv",
        )
        shutil.move(csv_file_path, processed_failed_path)

        if temp_output_exists:
            os.remove(temp_output_path)

        sys.exit(100)

    # B) Partial success
    if failed_any and transformed_any:
        log_event(logfile_path, "Partial success")

        transformed_rows = []
        with open(temp_output_path, newline="", encoding="utf-8") as temp_output_read_file:
            temp_output_reader = csv.DictReader(temp_output_read_file)
            for transformed_row in temp_output_reader:
                transformed_rows.append(transformed_row)

        append_to_duplicate_index(DUPLICATE_INDEX_PATH, transformed_rows)

        final_normalized_path = os.path.join(
            NORMALIZED_DIR,
            f"{run_timestamp}-{csv_filename}-partial.csv",
        )
        shutil.move(temp_output_path, final_normalized_path)

        processed_partial_path = os.path.join(
            PROCESSED_DIR,
            f"{run_timestamp}-{csv_filename}-partial.csv",
        )
        shutil.move(csv_file_path, processed_partial_path)

        sys.exit(101)

    # C) Full success
    if not failed_any and transformed_any:
        log_event(logfile_path, "Full success")

        transformed_rows = []
        with open(temp_output_path, newline="", encoding="utf-8") as temp_output_read_file:
            temp_output_reader = csv.DictReader(temp_output_read_file)
            for transformed_row in temp_output_reader:
                transformed_rows.append(transformed_row)

        append_to_duplicate_index(DUPLICATE_INDEX_PATH, transformed_rows)

        final_normalized_path = os.path.join(
            NORMALIZED_DIR,
            f"{run_timestamp}-{csv_filename}.csv",
        )
        shutil.move(temp_output_path, final_normalized_path)

        processed_success_path = os.path.join(
            PROCESSED_DIR,
            f"{run_timestamp}-{csv_filename}.csv",
        )
        shutil.move(csv_file_path, processed_success_path)

        sys.exit(0)

    # D) All full duplicates
    if not failed_any and not transformed_any:
        log_event(logfile_path, "All rows were FULL duplicates")

        processed_duplicates_path = os.path.join(
            PROCESSED_DIR,
            f"{run_timestamp}-{csv_filename}.csv",
        )
        shutil.move(csv_file_path, processed_duplicates_path)

        if temp_output_exists:
            os.remove(temp_output_path)

        sys.exit(0)


if __name__ == "__main__":
    main()
