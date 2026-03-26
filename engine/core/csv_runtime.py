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
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Prepare paths
# ---------------------------------------------------------------------------
def build_paths(
    data_dir: str,
    run_timestamp: str,
    csv_filename: str,
) -> dict[str, str]:
    """
    Construct all directory and file paths for a single pipeline run.

    Note:
    - duplicate_index_csv is a placeholder here.
      The real bank-specific path is set later in process_csv.py
      once the bank is known.
    """

    name_without_ext, _ = os.path.splitext(csv_filename)

    # ruff: noqa: E501
    # fmt: off
    return {
        # Directories
        "incoming_dir": os.path.join(data_dir, "incoming"),
        "processed_dir": os.path.join(data_dir, "processed"),
        "failed_dir": os.path.join(data_dir, "failed"),
        "normalized_dir": os.path.join(data_dir, "normalized"),
        "temp_dir": os.path.join(data_dir, "temp"),
        "duplicate_index_dir": os.path.join(data_dir, "duplicate-index"),
        "duplicate_index_backup_dir": os.path.join(data_dir, "duplicate-index", "backups"),

        # Duplicate index (placeholder, overwritten later)
        "duplicate_index_csv": os.path.join(data_dir, "duplicate-index", "UNSET.csv"),
        "duplicate_index_previous_csv": os.path.join(data_dir, "temp", "previous-duplicate-index.csv"),

        # Temporary normalized output
        "temp_normalized_csv": os.path.join(data_dir, "temp", f"{run_timestamp}-{name_without_ext}.tmp.csv"),

        # Failed rows
        "failed_normalize_csv": os.path.join(data_dir, "failed", f"{run_timestamp}-{name_without_ext}-normalize-failed.csv"),
        "failed_duplicate_csv": os.path.join(data_dir, "failed", f"{run_timestamp}-{name_without_ext}-duplicate-failed.csv"),

        # Processed originals
        "processed_failed_csv": os.path.join(data_dir, "processed", f"{run_timestamp}-{name_without_ext}-processed-failed.csv"),
        "processed_partial_csv": os.path.join(data_dir, "processed", f"{run_timestamp}-{name_without_ext}-processed-partial.csv"),
        "processed_success_csv": os.path.join(data_dir, "processed", f"{run_timestamp}-{name_without_ext}-processed.csv"),

        # Normalized output
        "normalized_partial_csv": os.path.join(data_dir, "normalized", f"{run_timestamp}-{name_without_ext}-normalized-partial.csv"),
        "normalized_success_csv": os.path.join(data_dir, "normalized", f"{run_timestamp}-{name_without_ext}-normalized.csv"),
    }
    # fmt: on
    # ruff: enable=E501


# ---------------------------------------------------------------------------
# Load CSV
# ---------------------------------------------------------------------------
def load_csv_rows(csv_file_path: str) -> list[dict[str, str]]:
    """
    Load CSV rows into a list of dictionaries.

    Encoding strategy:
    - Try UTF-8 first (most common)
    - Fallback to Windows-1252 (most common non-UTF-8 in BE/NL)
    - Fallback to UTF-16 (Excel "Unicode Text")

    Delimiter strategy:
    - Auto-detect via csv.Sniffer()
    - Fallback to semicolon

    Returns:
        List of dicts with raw column names.
    """

    # ------------------------------------------------------------
    # 1. Try reading file with different encodings
    # ------------------------------------------------------------
    encodings_to_try = ["utf-8-sig", "cp1252", "utf-16"]

    file_text = None
    used_encoding = None

    for enc in encodings_to_try:
        try:
            with open(csv_file_path, encoding=enc) as f:
                file_text = f.read()
            used_encoding = enc
            break
        except UnicodeDecodeError:
            continue

    if file_text is None:
        raise ValueError("Unable to decode CSV file with utf-8, cp1252, or utf-16.")

    # ------------------------------------------------------------
    # 2. Detect delimiter
    # ------------------------------------------------------------
    try:
        detected_dialect = csv.Sniffer().sniff(file_text[:4096])
    except csv.Error:
        detected_dialect = csv.excel
        detected_dialect.delimiter = ";"

    # ------------------------------------------------------------
    # 3. Parse CSV using detected encoding + dialect
    # ------------------------------------------------------------
    rows: list[dict[str, str]] = []

    with open(csv_file_path, encoding=used_encoding, newline="") as f:
        reader = csv.DictReader(f, dialect=detected_dialect)
        for row in reader:
            cleaned_row = {k: (v if v is not None else "") for k, v in row.items()}
            rows.append(cleaned_row)

    return rows


# ---------------------------------------------------------------------------
# Writer creation
# ---------------------------------------------------------------------------
def ensure_writer(path: str, writer_ref: dict[str, Any], fieldnames: list[str]) -> csv.DictWriter:
    """Lazily create a CSV writer and file handle. Headers written once."""
    if writer_ref.get("writer") is None:
        f = open(path, "w", newline="", encoding="utf-8")
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        writer_ref["writer"] = w
        writer_ref["file"] = f

    return writer_ref["writer"]


# ---------------------------------------------------------------------------
# Write a failed row
# ---------------------------------------------------------------------------
def write_failed_row(path: str, writer_ref: dict[str, Any], row: dict[str, Any]) -> None:
    """Write a failed row to the given CSV file."""
    writer = ensure_writer(path, writer_ref, list(row.keys()))
    writer.writerow(row)


# ---------------------------------------------------------------------------
# Load normalized rows
# ---------------------------------------------------------------------------
def load_normalized_rows(path: str) -> list[dict[str, Any]]:
    """Load all normalized rows from a temporary output file."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=";"))


# ---------------------------------------------------------------------------
# Load all bank configs
# ---------------------------------------------------------------------------
def load_all_bank_configs(config_dir: str) -> dict[str, dict[str, Any]]:
    """
    Load all YAML bank configuration files from the given directory.
    Returns a dict: bank_name -> config_dict
    """

    configs: dict[str, dict[str, Any]] = {}

    for filename in os.listdir(config_dir):
        if not filename.endswith(".yaml"):
            continue

        full_path = os.path.join(config_dir, filename)

        with open(full_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        bank_name = os.path.splitext(filename)[0]
        cfg["bank"] = bank_name

        configs[bank_name] = cfg

    return configs
