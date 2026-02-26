"""
Duplicate index management:
- Load duplicate index
- Append new rows
- Create timestamped backups
- Rotate old backups

This module contains all logic related to maintaining duplicate-index.csv.
"""

import os
import csv
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, DefaultDict, Callable
from collections import defaultdict


# ---------------------------------------------------------------------------
# Backup + rotation configuration
# ---------------------------------------------------------------------------
MAX_BACKUPS = 50
MAX_BACKUP_AGE_DAYS = 365
RUN_TS_FORMAT = "%Y%m%d-%H%M%S"


# ---------------------------------------------------------------------------
# Load duplicate index
# ---------------------------------------------------------------------------
def load_duplicate_index(duplicate_index_path: str) -> DefaultDict[str, List[Dict[str, str]]]:
    """
    Load the global duplicate index from CSV.
    Returns a dict: key → list of rows with that key.
    """
    index: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)

    if not os.path.exists(duplicate_index_path):
        return index

    with open(duplicate_index_path, newline="", encoding="utf-8") as index_file:
        reader = csv.DictReader(index_file)

        for row in reader:
            key = (row.get("BANKREFERENTIE") or "").strip()
            if key:
                index[key].append(row)

    return index


# ---------------------------------------------------------------------------
# Append new rows to duplicate index
# ---------------------------------------------------------------------------
def append_to_duplicate_index(duplicate_index_path: str, new_rows: List[Dict[str, str]]) -> None:
    """
    Append transformed rows to the global duplicate index.
    Creates the file with header if it does not exist.
    """
    if not new_rows:
        return

    file_exists = os.path.exists(duplicate_index_path)
    fieldnames = list(new_rows[0].keys())

    with open(duplicate_index_path, "a", newline="", encoding="utf-8") as index_file:
        writer = csv.DictWriter(index_file, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for row in new_rows:
            writer.writerow(row)


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
    Logs only on error. Never interrupts the processing flow.
    """
    try:
        if not os.path.exists(duplicate_index_path):
            return

        os.makedirs(backup_dir, exist_ok=True)

        backup_filename = f"{run_timestamp}-duplicate-index.csv"
        backup_path = os.path.join(backup_dir, backup_filename)

        shutil.copy2(duplicate_index_path, backup_path)

    except Exception as exc:
        log_event(logfile_path, f"[BACKUP ERROR] {exc}")


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
    Logs only on error. Never interrupts the processing flow.
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
                except Exception as exc:
                    log_event(logfile_path, f"[ROTATION ERROR] {exc}")
            else:
                kept.append((timestamp, filename))

        # Enforce MAX_BACKUPS
        if len(kept) > MAX_BACKUPS:
            excess = len(kept) - MAX_BACKUPS
            for timestamp, filename in kept[:excess]:
                try:
                    os.remove(os.path.join(backup_dir, filename))
                except Exception as exc:
                    log_event(logfile_path, f"[ROTATION ERROR] {exc}")

    except Exception as exc:
        log_event(logfile_path, f"[ROTATION ERROR] {exc}")
