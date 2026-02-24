import csv
import os
from typing import List, Dict, DefaultDict
from collections import defaultdict


# ---------------------------------------------------------------------------
# Load duplicate index
# ---------------------------------------------------------------------------
def load_duplicate_index(duplicate_index_path: str) -> DefaultDict[str, List[Dict[str, str]]]:
    """
    Load the global duplicate index from CSV.
    Returns a dict: key → list of rows with that key.
    """
    duplicate_index: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)

    if not os.path.exists(duplicate_index_path):
        return duplicate_index

    with open(duplicate_index_path, newline="", encoding="utf-8") as index_file:
        index_reader = csv.DictReader(index_file)

        for index_row in index_reader:
            key = (index_row.get("BANKREFERENTIE") or "").strip()
            if key:
                duplicate_index[key].append(index_row)

    return duplicate_index


# ---------------------------------------------------------------------------
# Append new rows to duplicate index
# ---------------------------------------------------------------------------
def append_to_duplicate_index(duplicate_index_path: str, new_rows: List[Dict[str, str]]) -> None:
    """
    Append transformed rows to the global duplicate index.
    Creates the file with header if it does not exist.
    """
    if not new_rows:
        return

    file_exists = os.path.exists(duplicate_index_path)
    fieldnames = list(new_rows[0].keys())

    with open(duplicate_index_path, "a", newline="", encoding="utf-8") as index_file:
        index_writer = csv.DictWriter(index_file, fieldnames=fieldnames)

        if not file_exists:
            index_writer.writeheader()

        for new_row in new_rows:
            index_writer.writerow(new_row)
