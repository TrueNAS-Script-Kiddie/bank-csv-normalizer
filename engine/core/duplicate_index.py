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
    Returns a dict: duplicate_key → list of rows with that key.
    """

    index: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)

    if not os.path.exists(duplicate_index_path):
        return index

    with open(duplicate_index_path, newline="", encoding="utf-8") as index_file:
        reader = csv.DictReader(index_file, delimiter=';')

        for row in reader:
            key = (row.get("duplicate_key") or "").strip()
            if key:
                index[key].append(row)

    return index


# ---------------------------------------------------------------------------
# Append new rows to updated_duplicate_index
# ---------------------------------------------------------------------------
def append_to_duplicate_index(duplicate_index_path: str, duplicate_index_rows: List[Dict[str, str]]) -> None:
    """
    Append new duplicate-index rows to the updated duplicate-index file.
    Creates the file with header if it does not exist.
    """

    if not duplicate_index_rows:
        return

    file_exists = os.path.exists(duplicate_index_path)
    fieldnames = list(duplicate_index_rows[0].keys())

    with open(duplicate_index_path, "a", newline="", encoding="utf-8") as index_file:
        writer = csv.DictWriter(index_file, fieldnames=fieldnames, delimiter=';')

        if not file_exists:
            writer.writeheader()

        for row in duplicate_index_rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# Create updated duplicate-index snapshot
# ---------------------------------------------------------------------------
def create_updated_duplicate_index(
    duplicate_index_path: str,
    backup_dir: str,
    run_timestamp: str,
    csv_filename: str,
    duplicate_index_rows: List[Dict[str, str]],
) -> str:
    """
    Create a timestamped updated duplicate-index file:
    - Copy existing duplicate-index.csv if present
    - Otherwise create empty base
    - Append duplicate_index_rows
    Returns the path to the updated duplicate index file.
    """

    # Path for updated snapshot
    bank_name = os.path.splitext(os.path.basename(duplicate_index_path))[0].replace("-duplicate-index", "")
    updated_duplicate_index = os.path.join(
        backup_dir,
        f"{run_timestamp}-{os.path.splitext(csv_filename)[0]}-{bank_name}-duplicate-index.csv"
    )

    # Base: existing dup-index or empty file
    if os.path.exists(duplicate_index_path):
        shutil.copy2(duplicate_index_path, updated_duplicate_index)

    # Append new rows
    append_to_duplicate_index(updated_duplicate_index, duplicate_index_rows)

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
    bank_config: Dict[str, Any],
) -> str:
    """
    Classify a row against the duplicate index using YAML rules.

    identical = all required fields match
    conflict  = key exists but required fields differ
    new       = key not present
    """

    existing_rows = duplicate_index.get(key, [])
    if not existing_rows:
        return "new"

    # Determine which fields must match
    required_fields = list(bank_config["columns"]["required"].keys())

    for existing in existing_rows:
        required_match = True

        for field in required_fields:
            if existing.get(field, "").strip() != row.get(field, "").strip():
                required_match = False
                break

        if required_match:
            return "identical"

    # Key exists, but no required-field match
    return "conflict"