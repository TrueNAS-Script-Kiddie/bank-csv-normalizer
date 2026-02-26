"""
CSV runtime helpers:
- CSV loading
- CSV structure validation
- Key extraction
- Writer creation
- Loading transformed rows

This module contains all CSV-related runtime infrastructure.
Nothing more, nothing less.
"""

import csv
from typing import Any, Dict, List, Optional


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
    key = csv_row.get("BANKREFERENTIE")
    if not key:
        return None

    key = key.strip()
    return key if key else None


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
# Load transformed rows
# ---------------------------------------------------------------------------
def load_transformed_rows(path: str) -> List[Dict[str, Any]]:
    """Load all transformed rows from a temporary output file."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
