import re
from typing import Any


# ---------------------------------------------------------------------------
# Validate CSV structure
# ---------------------------------------------------------------------------
def validate_and_prepare(
    csv_rows: list[dict[str, str]], bank_config: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """
    Validate CSV structure, apply filtering rules, and map CSV column names
    to internal field names based on the bank configuration.

    Returns:
        (validated_rows, column_map)

        validated_rows: list of rows with internal field names
        column_map: mapping from internal_name -> actual CSV column name

    Raises:
        ValueError: if required columns are missing or regex validation fails.
    """

    if not csv_rows:
        raise ValueError("CSV contains no rows.")

    header = list(csv_rows[0].keys())
    columns_cfg = bank_config["columns"]

    # ----------------------------------------------------------------------
    # 1. Build column_map: internal_name -> actual CSV column name
    # ----------------------------------------------------------------------
    column_map: dict[str, str] = {}

    # Required columns must exist
    for internal_name, cfg in columns_cfg["required"].items():
        matched_column = None

        for possible_name in cfg["names"]:
            if possible_name in header:
                matched_column = possible_name
                break

        if matched_column is None:
            raise ValueError(f"Missing required column: {internal_name} (expected one of {cfg['names']})")

        column_map[internal_name] = matched_column

    # Optional columns may or may not exist
    for internal_name, cfg in columns_cfg.get("optional", {}).items():
        matched_column = None

        for possible_name in cfg["names"]:
            if possible_name in header:
                matched_column = possible_name
                break

        if matched_column:
            column_map[internal_name] = matched_column

    # ----------------------------------------------------------------------
    # 2. Validate rows + apply filtering + regex checks
    # ----------------------------------------------------------------------
    validated_rows: list[dict[str, str]] = []

    for source_line, raw_row in enumerate(csv_rows, start=2):  # start=2: row 1 is the header
        mapped_row: dict[str, Any] = {}

        # Map CSV columns to internal names
        for internal_name, csv_name in column_map.items():
            mapped_row[internal_name] = raw_row.get(csv_name, "").strip()

        # Apply filter rules (exact-match list or regex)
        skip_row = False
        for internal_name, cfg in columns_cfg["required"].items():
            if "filter" in cfg:
                if mapped_row[internal_name] not in cfg["filter"]:
                    skip_row = True
                    break
            if "filter_regex" in cfg:
                if not re.fullmatch(cfg["filter_regex"], mapped_row[internal_name]):
                    skip_row = True
                    break

        if skip_row:
            continue

        # Regex validation
        for internal_name, cfg in columns_cfg["required"].items():
            if "regex" in cfg:
                pattern = re.compile(cfg["regex"])
                if not pattern.match(mapped_row[internal_name]):
                    raise ValueError(
                        f"Column '{internal_name}' failed regex validation: "
                        f"value='{mapped_row[internal_name]}' regex='{cfg['regex']}'"
                    )

        mapped_row["_source_line"] = source_line
        mapped_row["_original_csv_row"] = raw_row
        validated_rows.append(mapped_row)

    return validated_rows, column_map


# ---------------------------------------------------------------------------
# Extract unique key for duplicate detection
# ---------------------------------------------------------------------------
def extract_duplicate_key(row: dict[str, str], bank_config: dict[str, Any]) -> str | None:
    """
    Extract the duplicate detection key for a validated & mapped CSV row.

    The bank_config must contain:
        duplicate_key:
            columns: [list of internal column names]
            regex: optional regex to extract a sub-value

    Behaviour:
    - If regex is provided: try to extract from each column in order.
    - If regex is missing: concatenate the column values.
    - Returns None if no usable key can be produced.
    """

    cfg = bank_config.get("duplicate_key")
    if not cfg:
        raise ValueError("Bank config missing 'duplicate_key' section.")

    columns = cfg.get("columns", [])
    if not columns:
        raise ValueError("duplicate_key.columns must contain at least one column name.")

    # Collect values from the row for the configured columns
    column_values = []
    for col in columns:
        value = row.get(col, "")
        if value is None:
            value = ""
        column_values.append(value.strip())

    # ----------------------------------------------------------------------
    # 1. Regex extraction (preferred)
    # ----------------------------------------------------------------------
    regex_pattern = cfg.get("regex")

    if regex_pattern:
        pattern = re.compile(regex_pattern)

        for value in column_values:
            match = pattern.search(value)
            if match:
                extracted = match.group(1).strip()
                return extracted or None

        # Regex was provided but nothing matched → no key
        return None

    # ----------------------------------------------------------------------
    # 2. Fallback: concatenate column values
    # ----------------------------------------------------------------------
    combined = "|".join(v for v in column_values if v)
    combined = combined.strip()

    return combined or None


def autodetect_bank(csv_rows: list[dict[str, str]], all_bank_configs: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """
    Determine which bank configuration matches the CSV based on required columns.

    Rules:
    - A bank matches only if ALL required columns (any of their possible names)
      appear in the CSV header.
    - If exactly one bank matches → return that config.
    - If zero banks match → return None.
    - If multiple banks match → raise an error (ambiguous CSV).

    Returns:
        The selected bank config dict, or None if no match.
    """

    if not csv_rows:
        raise ValueError("CSV contains no rows, cannot detect bank.")

    csv_header = list(csv_rows[0].keys())
    matching_banks: list[dict[str, Any]] = []

    # ----------------------------------------------------------------------
    # Check each bank config
    # ----------------------------------------------------------------------
    for _bank_name, bank_cfg in all_bank_configs.items():
        columns_cfg = bank_cfg.get("columns", {})
        required_cfg = columns_cfg.get("required", {})

        all_required_present = True

        # Check each required internal field
        for _internal_name, col_cfg in required_cfg.items():
            possible_names = col_cfg.get("names", [])

            # Does ANY of the possible CSV column names exist?
            if not any(name in csv_header for name in possible_names):
                all_required_present = False
                break

        if all_required_present:
            matching_banks.append(bank_cfg)

    # ----------------------------------------------------------------------
    # Resolve match result
    # ----------------------------------------------------------------------
    if len(matching_banks) == 1:
        return matching_banks[0]

    if len(matching_banks) == 0:
        missing_per_bank = []
        for bank_name, bank_cfg in all_bank_configs.items():
            required_cfg = bank_cfg.get("columns", {}).get("required", {})
            missing = [
                f"{internal_name} (expected one of: {col_cfg.get('names', [])})"
                for internal_name, col_cfg in required_cfg.items()
                if not any(name in csv_header for name in col_cfg.get("names", []))
            ]
            if missing:
                missing_per_bank.append(f"{bank_name}: missing columns: {missing}")
            else:
                missing_per_bank.append(f"{bank_name}: all columns present (unexpected non-match)")
        raise ValueError(f"No bank config matches CSV headers {csv_header}. " + " | ".join(missing_per_bank))

    # More than one match → ambiguous CSV
    bank_names = [cfg.get("bank", "<unknown>") for cfg in matching_banks]
    raise ValueError(f"Ambiguous CSV: matches multiple banks: {bank_names}")
