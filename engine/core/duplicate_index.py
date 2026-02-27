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
from typing import List, Dict, DefaultDict, Callable, Any
from collections import defaultdict
from engine.core.runtime import log_event


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
# Append new rows to updated_duplicate_index
# ---------------------------------------------------------------------------
def append_to_duplicate_index(duplicate_index_path: str, normalized_rows: List[Dict[str, str]]) -> None:
    """
    Append normalized rows to the updated_duplicate_index.
    Creates the file with header if it does not exist.
    """
    if not normalized_rows:
        return

    file_exists = os.path.exists(duplicate_index_path)
    fieldnames = list(normalized_rows[0].keys())

    with open(duplicate_index_path, "a", newline="", encoding="utf-8") as index_file:
        writer = csv.DictWriter(index_file, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for row in normalized_rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Create updated duplicate-index snapshot
# ---------------------------------------------------------------------------
def create_updated_duplicate_index(
    duplicate_index_path: str,
    backup_dir: str,
    run_timestamp: str,
    csv_filename: str,
    normalized_rows: List[Dict[str, str]],
) -> str:
    """
    Create a timestamped updated duplicate-index file:
    - Copy existing duplicate-index.csv if present
    - Otherwise create empty base
    - Append normalized_rows
    Returns the path to the updated duplicate index file.
    """

    import os
    import shutil

    # Path for updated snapshot
    updated_duplicate_index = os.path.join(
        backup_dir,
        f"{run_timestamp}-{csv_filename}-duplicate-index.csv"
    )

    # Base: existing dup-index or empty file
    if os.path.exists(duplicate_index_path):
        shutil.copy2(duplicate_index_path, updated_duplicate_index)
    else:
        open(updated_duplicate_index, "w", encoding="utf-8").close()

    # Append new rows
    append_to_duplicate_index(updated_duplicate_index, normalized_rows)

    return updated_duplicate_index


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


# ---------------------------------------------------------------------------
# Classify rows against the duplicate index
# ---------------------------------------------------------------------------
def classify_duplicate(
    duplicate_index: Dict[str, List[Dict[str, Any]]],
    key: str,
    row: Dict[str, Any],
) -> str:
    """
    Classify a row against the duplicate index.

    Returns:
        "identical"  – same key and row already exists
        "conflict"   – same key but row differs
        "new"        – key not present
    """
    existing = duplicate_index.get(key, [])

    if any(r == row for r in existing):
        return "identical"

    if existing:
        return "conflict"

    return "new"