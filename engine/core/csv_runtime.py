"""
CSV runtime helpers:
- CSV loading
- CSV structure validation
- Key extraction
- Writer creation
- Loading normalized rows
- Path construction for a single pipeline run

This module contains all CSV-related runtime infrastructure.
Nothing more, nothing less.
"""

import csv
import os
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Prepare paths
# ---------------------------------------------------------------------------
def build_paths(
    data_dir: str,
    run_timestamp: str,
    csv_filename: str,
) -> Dict[str, str]:
    """
    Construct all directory and file paths for a single pipeline run.
    """

    return {
        # Directories
        "incoming_dir": os.path.join(data_dir, "incoming"),
        "processed_dir": os.path.join(data_dir, "processed"),
        "failed_dir": os.path.join(data_dir, "failed"),
        "normalized_dir": os.path.join(data_dir, "normalized"),
        "temp_dir": os.path.join(data_dir, "temp"),
        "duplicate_index_dir": os.path.join(data_dir, "duplicate-index"),
        "duplicate_index_backup_dir": os.path.join(data_dir, "duplicate-index", "backups"),

        # Duplicate index
        "duplicate_index_csv": os.path.join(data_dir, "duplicate-index", "duplicate-index.csv"),
        "duplicate_index_previous_csv": os.path.join(data_dir, "temp", "previous-duplicate-index.csv"),

        # Temporary normalized output
        "temp_normalized_csv": os.path.join(data_dir, "temp", f"{run_timestamp}-{csv_filename}.tmp.csv"),

        # Failed rows
        "failed_normalize_csv": os.path.join(data_dir, "failed", f"{run_timestamp}-{csv_filename}-normalize-failed.csv"),
        "failed_duplicate_csv": os.path.join(data_dir, "failed", f"{run_timestamp}-{csv_filename}-duplicate-failed.csv"),

        # Processed originals
        "processed_failed_csv": os.path.join(data_dir, "processed", f"{run_timestamp}-{csv_filename}-failed.csv"),
        "processed_partial_csv": os.path.join(data_dir, "processed", f"{run_timestamp}-{csv_filename}-partial.csv"),
        "processed_success_csv": os.path.join(data_dir, "processed", f"{run_timestamp}-{csv_filename}.csv"),

        # Normalized output
        "normalized_partial_csv": os.path.join(data_dir, "normalized", f"{run_timestamp}-{csv_filename}-partial.csv"),
        "normalized_success_csv": os.path.join(data_dir, "normalized", f"{run_timestamp}-{csv_filename}.csv"),
    }


# ---------------------------------------------------------------------------
# Load CSV
# ---------------------------------------------------------------------------
def load_csv_rows(csv_file_path: str) -> List[Dict[str, str]]:
    """Load CSV rows as a list of dictionaries."""
    rows: List[Dict[str, str]] = []

    with open(csv_file_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Validate CSV structure
# ---------------------------------------------------------------------------
def validate_csv_structure(csv_rows: List[Dict[str, str]]) -> bool:
    """
    Validate that the CSV contains the required columns.
    Extend this list as needed.
    """
    if not csv_rows:
        return False

    required_columns = ["BANKREFERENTIE"]
    return all(column in csv_rows[0] for column in required_columns)


# ---------------------------------------------------------------------------
# Extract unique key for duplicate detection
# ---------------------------------------------------------------------------
def extract_key(csv_row: Dict[str, str]) -> Optional[str]:
    """
    Extract the unique key used for duplicate detection.
    Returns None if missing or empty.
    """
    key = (csv_row.get("BANKREFERENTIE") or "").strip()
    return key or None


# ---------------------------------------------------------------------------
# Writer creation
# ---------------------------------------------------------------------------
def ensure_writer(
    path: str,
    writer_ref: Dict[str, Any],
    fieldnames: List[str]
) -> csv.DictWriter:
    """
    Lazily create a CSV writer and file handle.
    Ensures headers are written exactly once.
    """
    if writer_ref.get("writer") is None:
        f = open(path, "w", newline="", encoding="utf-8")
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        writer_ref["writer"] = w
        writer_ref["file"] = f

    return writer_ref["writer"]


# ---------------------------------------------------------------------------
# Write a failed row to the appropriate failure CSV
# ---------------------------------------------------------------------------
def write_failed_row(path: str, writer_ref: Dict[str, Any], row: Dict[str, Any]) -> None:
    """
    Write a failed row to the given CSV file, creating the writer on first use.
    """
    writer = ensure_writer(path, writer_ref, list(row.keys()))
    writer.writerow(row)


# ---------------------------------------------------------------------------
# Load normalized rows
# ---------------------------------------------------------------------------
def load_normalized_rows(path: str) -> List[Dict[str, Any]]:
    """Load all normalized rows from a temporary output file."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
