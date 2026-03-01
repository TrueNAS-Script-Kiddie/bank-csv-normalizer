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

from engine.core.duplicate_index import (
    create_updated_duplicate_index,
    rotate_duplicate_backups,
)
from engine.core.runtime import log_event, send_email


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
    outcome: str,
    normalized_rows: Optional[List[Dict[str, Any]]],
    message: str,
) -> None:
    """
    Perform all end-of-processing operations.
    This is the single exit path for the entire processing flow.
    """

    paths = context["paths"]
    csv_file_path = context["csv_file_path"]
    csv_filename = context["csv_filename"]
    run_timestamp = context["run_timestamp"]
    logfile_path = context["logfile_path"]

    # ----------------------------------------------------------------------
    # 1. Prepare duplicate-index update (not critical)
    # ----------------------------------------------------------------------
    updated_duplicate_index = None

    try:
        if normalized_rows:
            updated_duplicate_index = create_updated_duplicate_index(
                paths["duplicate_index_csv"],
                paths["duplicate_index_backup_dir"],
                run_timestamp,
                csv_filename,
                normalized_rows,
            )
    except Exception as e:
        log_email_exit(
            context,
            97,
            f"DUPLICATE INDEX PREP ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # ----------------------------------------------------------------------
    # 2. Move original CSV (critical)
    # ----------------------------------------------------------------------
    try:
        if outcome in ("structure_failed", "all_failed", "error"):
            final_csv_path = paths["processed_failed_csv"]
        elif outcome == "partial":
            final_csv_path = paths["processed_partial_csv"]
        else:
            final_csv_path = paths["processed_success_csv"]

        shutil.move(csv_file_path, final_csv_path)

    except Exception as e:
        try:
            shutil.move(csv_file_path, paths["processed_failed_csv"])
        except Exception:
            pass

        log_email_exit(
            context,
            94,
            f"ORIGINAL CSV MOVE ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # ----------------------------------------------------------------------
    # 3. Commit duplicate-index (critical)
    # ----------------------------------------------------------------------
    try:
        if normalized_rows:
            if os.path.exists(paths["duplicate_index_csv"]):
                shutil.copy2(paths["duplicate_index_csv"], paths["duplicate_index_previous_csv"])
            else:
                open(paths["duplicate_index_previous_csv"], "w", encoding="utf-8").close()

            shutil.copyfile(updated_duplicate_index, paths["duplicate_index_csv"])

    except Exception as e:
        try:
            shutil.move(final_csv_path, paths["processed_failed_csv"])
        except Exception:
            pass

        log_email_exit(
            context,
            93,
            f"DUPLICATE INDEX COMMIT ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # ----------------------------------------------------------------------
    # 4. Move normalized output (critical)
    # ----------------------------------------------------------------------
    try:
        if outcome == "partial":
            normalized_target = paths["normalized_partial_csv"]
        elif outcome == "success":
            normalized_target = paths["normalized_success_csv"]
        else:
            normalized_target = None

        if normalized_target and os.path.exists(paths["temp_normalized_csv"]):
            shutil.move(paths["temp_normalized_csv"], normalized_target)

    except Exception as e:
        if os.path.exists(paths["duplicate_index_previous_csv"]):
            try:
                shutil.copyfile(paths["duplicate_index_previous_csv"],paths["duplicate_index_csv"])
            except Exception:
                pass

        try:
            shutil.move(final_csv_path, paths["processed_failed_csv"])
        except Exception:
            pass

        log_email_exit(
            context,
            92,
            f"NORMALIZED OUTPUT MOVE ERROR: {e}\n\nTraceback:\n{traceback.format_exc()}",
        )

    # ----------------------------------------------------------------------
    # 5. Rotate duplicate-index backups (not critical)
    # ----------------------------------------------------------------------
    try:
        rotate_duplicate_backups(
            paths["duplicate_index_backup_dir"],
            context["log_event"],
            logfile_path,
        )
    except Exception:
        pass

    # ----------------------------------------------------------------------
    # 6. Close writers
    # ----------------------------------------------------------------------
    close_open_writers(context)

    # ----------------------------------------------------------------------
    # 7. Cleanup temp directory
    # ----------------------------------------------------------------------
    try:
        shutil.rmtree(paths["temp_dir"], ignore_errors=True)
    except Exception:
        pass

    # ----------------------------------------------------------------------
    # 8. Final log + email + exit
    # ----------------------------------------------------------------------
    log_email_exit(context, exit_code, message)
