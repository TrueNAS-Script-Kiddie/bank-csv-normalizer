import csv
from typing import List, Dict


# ---------------------------------------------------------------------------
# Load CSV
# ---------------------------------------------------------------------------
def load_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    """Load CSV rows as a list of dicts."""
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Validate CSV structure
# ---------------------------------------------------------------------------
def validate_csv_structure(rows: List[Dict[str, str]]) -> bool:
    """
    Validate that the CSV contains the required columns.
    Expand this as needed.
    """
    if not rows:
        return False

    required = ["BANKREFERENTIE"]  # TODO: expand with real required columns
    return all(col in rows[0] for col in required)


# ---------------------------------------------------------------------------
# Extract unique key for duplicate detection
# ---------------------------------------------------------------------------
def extract_key(row: Dict[str, str]) -> str:
    """Extract the unique key used for duplicate detection."""
    return (row.get("BANKREFERENTIE") or "").strip()
