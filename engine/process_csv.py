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


def log_event(logfile: str, message: str) -> None:
    """Append timestamped log entry."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(f"{ts} {message}\n")


def main():
    # -------------------------------------------------------------------------
    # Arguments
    # -------------------------------------------------------------------------
    if len(sys.argv) != 4:
        print("Usage: process_csv.py <csv_path> <run_timestamp> <logfile_path>")
        sys.exit(1)

    csv_path = sys.argv[1]
    run_ts = sys.argv[2]
    logfile = sys.argv[3]
    csv_filename = os.path.basename(csv_path)

    # -------------------------------------------------------------------------
    # Directory structure (relative to BASE_DIR)
    # -------------------------------------------------------------------------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(BASE_DIR, "data")

    DIR_INCOMING = os.path.join(DATA_DIR, "incoming")
    DIR_PROCESSED = os.path.join(DATA_DIR, "processed")
    DIR_FAILED = os.path.join(DATA_DIR, "failed")
    DIR_NORMALIZED = os.path.join(DATA_DIR, "normalized")
    DIR_TEMP = os.path.join(DATA_DIR, "temp")
    DIR_DUP = os.path.join(DATA_DIR, "duplicate-index")

    DUP_INDEX_PATH = os.path.join(DIR_DUP, "duplicate-index.csv")

    # TEMP must exist only during runtime
    os.makedirs(DIR_TEMP, exist_ok=True)

    # -------------------------------------------------------------------------
    # Load CSV
    # -------------------------------------------------------------------------
    try:
        rows = load_csv_rows(csv_path)
        log_event(logfile, f"Loaded {len(rows)} rows")
    except Exception as e:
        log_event(logfile, f"ERROR loading CSV: {e}")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Validate structure
    # -------------------------------------------------------------------------
    if not validate_csv_structure(rows):
        log_event(logfile, "CSV structure/content check FAILED")

        target = os.path.join(
            DIR_PROCESSED,
            f"{run_ts}-{csv_filename}-failed.csv"
        )
        shutil.move(csv_path, target)

        sys.exit(100)

    log_event(logfile, "CSV structure/content check PASSED")

    # -------------------------------------------------------------------------
    # Load duplicate index
    # -------------------------------------------------------------------------
    duplicate_index = load_duplicate_index(DUP_INDEX_PATH)
    log_event(
        logfile,
        f"Loaded duplicate index with {sum(len(v) for v in duplicate_index.values())} rows"
    )

    failed_any = False
    transformed_any = False

    # TEMP output file
    temp_output_path = os.path.join(
        DIR_TEMP,
        f"{run_ts}-{csv_filename}.tmp.csv"
    )
    temp_file = open(temp_output_path, "w", newline="", encoding="utf-8")
    temp_writer = None

    # Failed rows
    transform_failed_path = os.path.join(
        DIR_FAILED,
        f"{run_ts}-{csv_filename}-transform-failed_rows.csv"
    )
    duplicate_failed_path = os.path.join(
        DIR_FAILED,
        f"{run_ts}-{csv_filename}-duplicate_rows.csv"
    )

    transform_failed_file = None
    transform_failed_writer = None
    duplicate_failed_file = None
    duplicate_failed_writer = None

    # -------------------------------------------------------------------------
    # Row-by-row processing
    # -------------------------------------------------------------------------
    try:
        for row in rows:

            # Transform
            try:
                transformed = transform_row(row)
            except Exception:
                failed_any = True
                if transform_failed_writer is None:
                    transform_failed_file = open(
                        transform_failed_path, "w", newline="", encoding="utf-8"
                    )
                    transform_failed_writer = csv.DictWriter(
                        transform_failed_file, fieldnames=list(row.keys())
                    )
                    transform_failed_writer.writeheader()
                transform_failed_writer.writerow(row)
                continue

            key = extract_key(transformed)
            if not key:
                failed_any = True
                if transform_failed_writer is None:
                    transform_failed_file = open(
                        transform_failed_path, "w", newline="", encoding="utf-8"
                    )
                    transform_failed_writer = csv.DictWriter(
                        transform_failed_file, fieldnames=list(row.keys())
                    )
                    transform_failed_writer.writeheader()
                transform_failed_writer.writerow(row)
                continue

            # Duplicate check
            existing_rows = duplicate_index.get(key, [])
            is_identical = any(existing == transformed for existing in existing_rows)

            if is_identical:
                log_event(logfile, f"IGNORED identical row with key {key}")
                continue

            if existing_rows:
                failed_any = True
                if duplicate_failed_writer is None:
                    duplicate_failed_file = open(
                        duplicate_failed_path, "w", newline="", encoding="utf-8"
                    )
                    duplicate_failed_writer = csv.DictWriter(
                        duplicate_failed_file, fieldnames=list(transformed.keys())
                    )
                    duplicate_failed_writer.writeheader()
                duplicate_failed_writer.writerow(transformed)
                log_event(logfile, f"DUPLICATE key (non-identical) {key}")
                continue

            # Not a duplicate → transformed
            transformed_any = True
            if temp_writer is None:
                temp_writer = csv.DictWriter(
                    temp_file, fieldnames=list(transformed.keys())
                )
                temp_writer.writeheader()
            temp_writer.writerow(transformed)

            # Update in-memory index
            duplicate_index[key].append(transformed)

    except Exception as e:
        log_event(logfile, f"ERROR during row processing: {e}")
        traceback.print_exc()
        sys.exit(1)

    finally:
        temp_file.close()
        if transform_failed_file:
            transform_failed_file.close()
        if duplicate_failed_file:
            duplicate_failed_file.close()

    # -------------------------------------------------------------------------
    # Final classification
    # -------------------------------------------------------------------------
    temp_exists = os.path.exists(temp_output_path)
    temp_empty = False

    if temp_exists:
        with open(temp_output_path, newline="", encoding="utf-8") as f:
            temp_empty = (sum(1 for _ in f) <= 1)

    # A) All failed
    if failed_any and not transformed_any:
        log_event(logfile, "All rows failed")

        target = os.path.join(
            DIR_PROCESSED,
            f"{run_ts}-{csv_filename}-failed.csv"
        )
        shutil.move(csv_path, target)

        if temp_exists:
            os.remove(temp_output_path)

        sys.exit(100)

    # B) Partial success
    if failed_any and transformed_any:
        log_event(logfile, "Partial success")

        transformed_rows = []
        with open(temp_output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            transformed_rows.extend(reader)

        append_to_duplicate_index(DUP_INDEX_PATH, transformed_rows)

        final_path = os.path.join(
            DIR_NORMALIZED,
            f"{run_ts}-{csv_filename}-partial.csv"
        )
        shutil.move(temp_output_path, final_path)

        target = os.path.join(
            DIR_PROCESSED,
            f"{run_ts}-{csv_filename}-partial.csv"
        )
        shutil.move(csv_path, target)

        sys.exit(101)

    # C) Full success
    if not failed_any and transformed_any:
        log_event(logfile, "Full success")

        transformed_rows = []
        with open(temp_output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            transformed_rows.extend(reader)

        append_to_duplicate_index(DUP_INDEX_PATH, transformed_rows)

        final_path = os.path.join(
            DIR_NORMALIZED,
            f"{run_ts}-{csv_filename}.csv"
        )
        shutil.move(temp_output_path, final_path)

        target = os.path.join(
            DIR_PROCESSED,
            f"{run_ts}-{csv_filename}.csv"
        )
        shutil.move(csv_path, target)

        sys.exit(0)

    # D) All full duplicates
    if not failed_any and not transformed_any:
        log_event(logfile, "All rows were FULL duplicates")

        target = os.path.join(
            DIR_PROCESSED,
            f"{run_ts}-{csv_filename}.csv"
        )
        shutil.move(csv_path, target)

        if temp_exists:
            os.remove(temp_output_path)

        sys.exit(0)


if __name__ == "__main__":
    main()
