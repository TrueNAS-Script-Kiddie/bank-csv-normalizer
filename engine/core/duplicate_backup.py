import os
import shutil
from datetime import datetime, timedelta
from typing import Callable


# ---------------------------------------------------------------------------
# Backup + rotation configuration
# ---------------------------------------------------------------------------
MAX_BACKUPS = 50
MAX_BACKUP_AGE_DAYS = 365
RUN_TS_FORMAT = "%Y%m%d-%H%M%S"


# ---------------------------------------------------------------------------
# Create a timestamped backup of duplicate-index.csv
# ---------------------------------------------------------------------------
def backup_duplicate_index(
    duplicate_index_path: str,
    backup_dir: str,
    run_timestamp: str,
    log_event: Callable[[str, str], None],
    logfile_path: str,
) -> None:
    """
    Create a timestamped backup of duplicate-index.csv.
    Logs only on error. Never interrupts the pipeline.
    """
    try:
        if not os.path.exists(duplicate_index_path):
            return

        os.makedirs(backup_dir, exist_ok=True)

        backup_filename = f"{run_timestamp}-duplicate-index.csv"
        backup_path = os.path.join(backup_dir, backup_filename)

        shutil.copy2(duplicate_index_path, backup_path)

    except Exception as exception:
        log_event(logfile_path, f"[BACKUP ERROR] {exception}")


# ---------------------------------------------------------------------------
# Rotate old backups (by age and count)
# ---------------------------------------------------------------------------
def rotate_duplicate_backups(
    backup_dir: str,
    log_event: Callable[[str, str], None],
    logfile_path: str,
) -> None:
    """
    Rotate old duplicate-index backups based on age and count.
    Logs only on error. Never interrupts the pipeline.
    """
    try:
        if not os.path.exists(backup_dir):
            return

        backup_files = []

        for filename in os.listdir(backup_dir):
            if filename.endswith("-duplicate-index.csv"):
                ts_part = filename[:-len("-duplicate-index.csv")]
                try:
                    timestamp = datetime.strptime(ts_part, RUN_TS_FORMAT)
                    backup_files.append((timestamp, filename))
                except ValueError:
                    # Ignore files that don't match the timestamp format
                    continue

        # Sort oldest → newest
        backup_files.sort(key=lambda x: x[0])

        # Remove backups older than MAX_BACKUP_AGE_DAYS
        cutoff = datetime.now() - timedelta(days=MAX_BACKUP_AGE_DAYS)
        kept = []

        for timestamp, filename in backup_files:
            if timestamp < cutoff:
                try:
                    os.remove(os.path.join(backup_dir, filename))
                except Exception as exception:
                    log_event(logfile_path, f"[ROTATION ERROR] {exception}")
            else:
                kept.append((timestamp, filename))

        # Enforce MAX_BACKUPS
        if len(kept) > MAX_BACKUPS:
            excess = len(kept) - MAX_BACKUPS
            for timestamp, filename in kept[:excess]:
                try:
                    os.remove(os.path.join(backup_dir, filename))
                except Exception as exception:
                    log_event(logfile_path, f"[ROTATION ERROR] {exception}")

    except Exception as exception:
        log_event(logfile_path, f"[ROTATION ERROR] {exception}")
