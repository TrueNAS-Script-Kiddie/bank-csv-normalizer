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

from core.duplicate_index import (
    append_to_duplicate_index,
    backup_duplicate_index,
    rotate_duplicate_backups,
)
from core.email_notifications import send_email


# ---------------------------------------------------------------------------
# Logging + email + exit
# ---------------------------------------------------------------------------
def log_email_exit(context: Dict[str, Any], exit_code: int, message: str) -> None:
    """Write final log entry, send email, then exit."""

    log_event = context["log_event"]
    logfile_path = context["logfile_path"]
    csv_filename = context["csv_filename"]
    run_timestamp = context["run_timestamp"]

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
def close_open_writers(context: Dict[str, Any]) -> None:
    """Close all file handles associated with CSV writers."""
    for ref in context["open_writers"]:
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
    context: Dict[str, Any],
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

    # Extract context fields
    csv_file_path = context["csv_file_path"]
    csv_filename = context["csv_filename"]
    run_timestamp = context["run_timestamp"]
    logfile_path = context["logfile_path"]
    temp_output_path = context["temp_output_path"]

    dirs = context["directories"]
    PROCESSED_DIR = dirs["processed"]
    FAILED_DIR = dirs["failed"]
    NORMALIZED_DIR = dirs["normalized"]
    TEMP_DIR = dirs["temp"]
    DUPLICATE_INDEX_PATH = dirs["duplicate_index"]
    BACKUP_DIR = dirs["backup"]

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
            context,
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
            context,
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
            context,
            94,
            f"ORIGINAL CSV MOVE ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # Close all writers
    close_open_writers(context)

    # Cleanup temp directory
    try:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    except Exception:
        pass

    # Final log + email + exit
    log_email_exit(context, exit_code, message)
    