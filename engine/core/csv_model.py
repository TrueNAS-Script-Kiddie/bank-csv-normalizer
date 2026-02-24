import csv
from typing import List, Dict


# ---------------------------------------------------------------------------
# Load CSV
# ---------------------------------------------------------------------------
def load_csv_rows(csv_file_path: str) -> List[Dict[str, str]]:
    """Load CSV rows as a list of dicts."""
    csv_rows: List[Dict[str, str]] = []

    with open(csv_file_path, newline="", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for csv_row in csv_reader:
            csv_rows.append(csv_row)

    return csv_rows


# ---------------------------------------------------------------------------
# Validate CSV structure
# ---------------------------------------------------------------------------
def validate_csv_structure(csv_rows: List[Dict[str, str]]) -> bool:
    """
    Validate that the CSV contains the required columns.
    Expand this as needed.
    """
    if not csv_rows:
        return False

    required_columns = ["BANKREFERENTIE"]  # TODO: expand with real required columns
    return all(column in csv_rows[0] for column in required_columns)


# ---------------------------------------------------------------------------
# Extract unique key for duplicate detection
# ---------------------------------------------------------------------------
def extract_key(csv_row: Dict[str, str]) -> str:
    """Extract the unique key used for duplicate detection."""
    return (csv_row.get("BANKREFERENTIE") or "").strip()
