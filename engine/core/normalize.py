from typing import Dict


# ---------------------------------------------------------------------------
# Normalize a single row
# ---------------------------------------------------------------------------
def normalize_row(csv_row: Dict[str, str]) -> Dict[str, str]:
    """
    Apply normalization logic to a single CSV row.
    Expand this with real normalization rules.
    """
    row = dict(csv_row) # shallow copy 
    # TODO: implement real normalization logic on `row` 
    return row
