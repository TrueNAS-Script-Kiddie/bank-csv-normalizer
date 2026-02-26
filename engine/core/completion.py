"""
Completion module:
Handles ALL end-of-processing operations:
- Duplicate index update + backup + rotation
- Moving normalized output
- Moving original CSV
- Closing open file handles
- Cleaning up the temp directory
- Final log + email + exit

This module is the single exit path for the entire processing flow.
"""

import os
import sys
import shutil
import traceback
from typing import Any, Dict, List, Optional

from core.duplicate_index import append_to_duplicate_index
from core.duplicate_index import backup_duplicate_index, rotate_duplicate_backups
from core.email_notifications import send_email


# These globals are set by process_csv.py before calling finalize()
csv_file_path: str
csv_filename: str
run_timestamp: str
logfile_path: str
temp_output_path: Optional[str]

PROCESSED_DIR: str
NORMALIZED_DIR: str
FAILED_DIR: str
TEMP_DIR: str
DUPLICATE_INDEX_PATH: str
BACKUP_DIR: str

# Writers passed in from process_csv.py
open_writers: List[Any] = []


# ---------------------------------------------------------------------------
# Logging + email + exit
# ---------------------------------------------------------------------------
def log_email_exit(exit_code: int, message: str) -> None:
    """Write final log entry, send email, then exit."""
    from process_csv import log_event  # avoid circular import

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


# ---------------------------------------------------------------------------
# Close all open writers
# ---------------------------------------------------------------------------
def close_open_writers() -> None:
    """Close all file handles associated with CSV writers."""
    for ref in open_writers:
        f = ref.get("file")
        if f:
            try:
                f.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Finalization
# ---------------------------------------------------------------------------
def finalize(
    exit_code: int,
    move_suffix: str,
    normalized_suffix: Optional[str],
    transformed_rows: Optional[List[Dict[str, Any]]],
    message: str,
) -> None:
    """
    Perform all end-of-processing operations.
    This is the ONLY exit path for the entire processing flow.
    """

    # Duplicate index update
    try:
        if transformed_rows:
            append_to_duplicate_index(DUPLICATE_INDEX_PATH, transformed_rows)
            backup_duplicate_index(
                DUPLICATE_INDEX_PATH,
                BACKUP_DIR,
                run_timestamp,
                lambda p, m: None,  # no-op logger
                logfile_path,
            )
            rotate_duplicate_backups(
                BACKUP_DIR,
                lambda p, m: None,
                logfile_path,
            )
    except Exception as e:
        log_email_exit(
            97,
            f"DUPLICATE INDEX ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # Move normalized output
    try:
        if normalized_suffix is not None and temp_output_path:
            normalized_output_path = os.path.join(
                NORMALIZED_DIR,
                f"{run_timestamp}-{csv_filename}{normalized_suffix}",
            )
            shutil.move(temp_output_path, normalized_output_path)
    except Exception as e:
        log_email_exit(
            96,
            f"NORMALIZED OUTPUT MOVE ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # Move original CSV
    try:
        final_csv_path = os.path.join(
            PROCESSED_DIR,
            f"{run_timestamp}-{csv_filename}{move_suffix}",
        )
        shutil.move(csv_file_path, final_csv_path)
    except Exception as e:
        log_email_exit(
            94,
            f"ORIGINAL CSV MOVE ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # Close all writers
    close_open_writers()

    # Cleanup temp directory
    try:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    except Exception:
        pass

    # Final log + email + exit
    log_email_exit(exit_code, message)
