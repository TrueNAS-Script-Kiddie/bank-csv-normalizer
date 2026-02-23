import csv
import os
from typing import List, Dict, DefaultDict
from collections import defaultdict


# ---------------------------------------------------------------------------
# Load duplicate index
# ---------------------------------------------------------------------------
def load_duplicate_index(path: str) -> DefaultDict[str, List[Dict[str, str]]]:
    """
    Load the global duplicate index from CSV.
    Returns a dict: key → list of rows with that key.
    """
    index: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)

    if not os.path.exists(path):
        return index

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Key extraction is done by process_csv via csv_model.extract_key
            # but we re‑extract here for safety.
            key = (row.get("BANKREFERENTIE") or "").strip()
            if key:
                index[key].append(row)

    return index


# ---------------------------------------------------------------------------
# Append new rows to duplicate index
# ---------------------------------------------------------------------------
def append_to_duplicate_index(path: str, rows: List[Dict[str, str]]) -> None:
    """
    Append transformed rows to the global duplicate index.
    Creates the file with header if it does not exist.
    """
    if not rows:
        return

    file_exists = os.path.exists(path)
    fieldnames = list(rows[0].keys())

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for row in rows:
            writer.writerow(row)
