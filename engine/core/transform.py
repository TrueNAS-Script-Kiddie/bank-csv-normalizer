from typing import Dict


# ---------------------------------------------------------------------------
# Transform a single row
# ---------------------------------------------------------------------------
def transform_row(csv_row: Dict[str, str]) -> Dict[str, str]:
    """
    Apply transformation logic to a single CSV row.
    Expand this with real transformation rules.
    """
    row = dict(csv_row) # shallow copy 
    # TODO: implement real transformation logic on `row` 
    return row
