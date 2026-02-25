#!/usr/bin/env python3
import sys
import os
import csv
import shutil
import traceback
from datetime import datetime

from core.csv_model import load_csv_rows, validate_csv_structure, extract_key
from core.transform_logic import transform_row
from core.duplicate_index import load_duplicate_index, append_to_duplicate_index
from core.duplicate_backup import backup_duplicate_index, rotate_duplicate_backups
from core.email_notifications import send_email


# -------------------------------------------------------------------------
# Directory configuration
# -------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
FAILED_DIR = os.path.join(DATA_DIR, "failed")
NORMALIZED_DIR = os.path.join(DATA_DIR, "normalized")
TEMP_DIR = os.path.join(DATA_DIR, "temp")
DUPLICATE_INDEX_DIR = os.path.join(DATA_DIR, "duplicate-index")
BACKUP_DIR = os.path.join(DUPLICATE_INDEX_DIR, "backups")

DUPLICATE_INDEX_PATH = os.path.join(DUPLICATE_INDEX_DIR, "duplicate-index.csv")


# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
def log_event(logfile_path: str, message: str) -> None:
    """Append a timestamped log message to the logfile."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile_path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} {message}\n")


# -------------------------------------------------------------------------
# Shared final action: log + email + exit
# -------------------------------------------------------------------------
def log_email_exit(exit_code: int, message: str):
    """Log a final message, send an email with filename/timestamp added, then exit."""
    log_event(logfile_path, message)

    subject = message
    body = (
        f"File: {csv_filename}\n"
        f"Timestamp: {run_timestamp}\n"
        f"{message}"
    )

    send_email(
        subject=subject,
        body=body,
        log_event=log_event,
        logfile_path=logfile_path,
    )

    sys.exit(exit_code)


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
def ensure_writer(path: str, writer_ref: dict, fieldnames: list[str]):
    """Lazily create a CSV writer and file handle."""
    if writer_ref["writer"] is None:
        f = open(path, "w", newline="", encoding="utf-8")
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        writer_ref["writer"] = w
        writer_ref["file"] = f
    return writer_ref["writer"]


def load_transformed_rows(path: str) -> list[dict]:
    """Load all transformed rows from a temp output file."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# -------------------------------------------------------------------------
# Finalization
# -------------------------------------------------------------------------
def finalize(
    exit_code: int,
    move_suffix: str,
    normalized_suffix: str | None,
    transformed_rows: list[dict] | None,
    message: str,
):
    """Perform file/index operations, then delegate logging+email+exit."""

    # Update duplicate index if needed
    if transformed_rows:
        append_to_duplicate_index(DUPLICATE_INDEX_PATH, transformed_rows)
        backup_duplicate_index(
            DUPLICATE_INDEX_PATH,
            BACKUP_DIR,
            run_timestamp,
            log_event,
            logfile_path
        )
        rotate_duplicate_backups(BACKUP_DIR, log_event, logfile_path)

    # Normalized output
    if normalized_suffix is not None:
        normalized_output_path = os.path.join(
            NORMALIZED_DIR,
            f"{run_timestamp}-{csv_filename}{normalized_suffix}"
        )
        shutil.move(temp_output_path, normalized_output_path)
    else:
        if os.path.exists(temp_output_path):
            os.remove(temp_output_path)

    # Move original CSV
    final_csv_path = os.path.join(
        PROCESSED_DIR,
        f"{run_timestamp}-{csv_filename}{move_suffix}"
    )
    shutil.move(csv_file_path, final_csv_path)

    # Final log + email + exit
    log_email_exit(exit_code, message)


# -------------------------------------------------------------------------
# Main
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

    for d in (PROCESSED_DIR, FAILED_DIR, NORMALIZED_DIR, TEMP_DIR, DUPLICATE_INDEX_DIR, BACKUP_DIR):
        os.makedirs(d, exist_ok=True)

    # Load CSV
    try:
        csv_rows = load_csv_rows(csv_file_path)
    except Exception as e:
        log_email_exit(
            exit_code=1,
            message=f"CSV LOAD ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )
    log_event(logfile_path, f"Loaded {len(csv_rows)} rows")

    # Validate structure
    if not validate_csv_structure(csv_rows):
        return finalize(
            exit_code=65,
            move_suffix="-failed.csv",
            normalized_suffix=None,
            transformed_rows=None,
            message="CSV STRUCTURE FAILED",
        )

    log_event(logfile_path, "CSV structure/content check PASSED")

    # Load duplicate index
    duplicate_index = load_duplicate_index(DUPLICATE_INDEX_PATH)
    log_event(
        logfile_path,
        f"Loaded duplicate index with {sum(len(v) for v in duplicate_index.values())} rows",
    )

    failed_any = False
    transformed_any = False

    # Prepare writers
    temp_output_path = os.path.join(TEMP_DIR, f"{run_timestamp}-{csv_filename}.tmp.csv")
    temp_output_file = open(temp_output_path, "w", newline="", encoding="utf-8")
    temp_output_writer = None

    transform_failed_ref = {"writer": None, "file": None}
    duplicate_failed_ref = {"writer": None, "file": None}

    transform_failed_path = os.path.join(FAILED_DIR, f"{run_timestamp}-{csv_filename}-transform-failed_rows.csv")
    duplicate_failed_path = os.path.join(FAILED_DIR, f"{run_timestamp}-{csv_filename}-duplicate_rows.csv")

    # Row processing
    try:
        for csv_row in csv_rows:

            # Transform
            try:
                transformed_row = transform_row(csv_row)
            except Exception:
                failed_any = True
                w = ensure_writer(transform_failed_path, transform_failed_ref, list(csv_row.keys()))
                w.writerow(csv_row)
                log_event(logfile_path, "Transform failed for a row")
                continue

            key = extract_key(transformed_row)
            if not key:
                failed_any = True
                w = ensure_writer(transform_failed_path, transform_failed_ref, list(csv_row.keys()))
                w.writerow(csv_row)
                log_event(logfile_path, "Missing BANKREFERENTIE for a row")
                continue

            # Duplicate check
            existing_rows = duplicate_index.get(key, [])
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
                temp_output_writer = csv.DictWriter(temp_output_file, fieldnames=list(transformed_row.keys()))
                temp_output_writer.writeheader()
            temp_output_writer.writerow(transformed_row)

            duplicate_index[key].append(transformed_row)

    except Exception as e:
        log_email_exit(
            exit_code=1,
            message=f"CSV PROCESSING ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    finally:
        temp_output_file.close()
        if transform_failed_ref["file"]:
            transform_failed_ref["file"].close()
        if duplicate_failed_ref["file"]:
            duplicate_failed_ref["file"].close()

    # Final classification

    # All failed
    if failed_any and not transformed_any:
        return finalize(
            exit_code=65,
            move_suffix="-failed.csv",
            normalized_suffix=None,
            transformed_rows=None,
            message="CSV FAILED (ALL ROWS)",
        )

    # Partial success
    if failed_any and transformed_any:
        transformed_rows = load_transformed_rows(temp_output_path)
        return finalize(
            exit_code=75,
            move_suffix="-partial.csv",
            normalized_suffix="-partial.csv",
            transformed_rows=transformed_rows,
            message="CSV PARTIAL SUCCESS",
        )

    # Full success
    if not failed_any and transformed_any:
        transformed_rows = load_transformed_rows(temp_output_path)
        return finalize(
            exit_code=0,
            move_suffix=".csv",
            normalized_suffix=".csv",
            transformed_rows=transformed_rows,
            message="CSV SUCCESS",
        )

    # All duplicates
    return finalize(
        exit_code=0,
        move_suffix=".csv",
        normalized_suffix=None,
        transformed_rows=None,
        message="CSV FULL DUPLICATES",
    )


if __name__ == "__main__":
    main()
