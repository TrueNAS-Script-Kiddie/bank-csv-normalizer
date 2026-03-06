import re
from typing import Dict, Any


DATE_RE = re.compile(r"^([0-9]{2})/([0-9]{2})/([0-9]{4})$")
TIME_RE = re.compile(r"\b([0-9]{2}:[0-9]{2})\b")
BANKREF_RE = re.compile(r"BANKREFERENTIE\s*:\s*([0-9]{16})")
DETAILS_VALDATE_RE = re.compile(r"VALUTADATUM\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})")
DETAILS_TRXDATE_RE = re.compile(r"([0-9]{2}/[0-9]{2}/[0-9]{4})")


def _parse_date_ddmmyyyy(value: str) -> str:
    """Convert DD/MM/YYYY → YYYY-MM-DD."""
    match = DATE_RE.match(value)
    if not match:
        raise ValueError(f"Invalid date format: {value}")
    dd, mm, yyyy = match.groups()
    return f"{yyyy}-{mm}-{dd}"


def normalize_row(csv_row: Dict[str, str]) -> Dict[str, Any]:
    """
    STEP 1 NORMALIZATION:
    Produces a Firefly‑compatible normalized row with all 13 required fields
    + 1 debug field for remaining details.
    """

    # ----------------------------------------------------------------------
    # Initialize Firefly‑compatible output structure
    # ----------------------------------------------------------------------
    normalized: Dict[str, Any] = {
        "external_id": "",
        "primary_transaction_date": "",
        "booking_date": "",
        "amount": "",
        "account_currency_code": "",
        "asset_account_iban": "",
        "asset_account_bic": "",
        "asset_account_name": "",
        "opposing_account_iban": "",
        "opposing_account_bic": "",
        "opposing_account_name": "",
        "description": "",
        "notes": "",
        "debug_details_rest": "",   # TEMPORARY DEBUG COLUMN
    }

    # ----------------------------------------------------------------------
    # Working copies
    # ----------------------------------------------------------------------
    details_raw = csv_row.get("details_raw", "")
    details_rest = details_raw

    # ----------------------------------------------------------------------
    # Amount
    # ----------------------------------------------------------------------
    normalized["amount"] = csv_row["amount"].replace(",", ".").strip()

    # ----------------------------------------------------------------------
    # Currency → account_currency_code
    # ----------------------------------------------------------------------
    normalized["account_currency_code"] = csv_row["currency"].strip()

    # ----------------------------------------------------------------------
    # Asset account IBAN
    # ----------------------------------------------------------------------
    normalized["asset_account_iban"] = (
        csv_row["account_iban"].replace(" ", "").upper()
    )

    # ----------------------------------------------------------------------
    # External ID (bank reference)
    # ----------------------------------------------------------------------
    m = BANKREF_RE.search(details_raw)
    if m:
        normalized["external_id"] = m.group(1)
        details_rest = details_rest.replace(m.group(0), "")

    # ----------------------------------------------------------------------
    # Booking date (value_date) + validation
    # ----------------------------------------------------------------------
    booking_date = _parse_date_ddmmyyyy(csv_row["value_date"])
    normalized["booking_date"] = booking_date

    m = DETAILS_VALDATE_RE.search(details_raw)
    if m:
        details_val_date = _parse_date_ddmmyyyy(m.group(1))
        if details_val_date != booking_date:
            raise ValueError(
                f"Booking date mismatch: csv={booking_date} details={details_val_date}"
            )
        details_rest = details_rest.replace(m.group(0), "")

    # ----------------------------------------------------------------------
    # Primary transaction date (execution_date + time)
    # ----------------------------------------------------------------------
    primary_date = _parse_date_ddmmyyyy(csv_row["execution_date"])

    # Extract time
    m_time = TIME_RE.search(details_raw)
    if m_time:
        hhmm = m_time.group(1)
        details_rest = details_rest.replace(hhmm, "")
    else:
        # Geen tijd in details → default naar 00:00
        hhmm = "00:00"

    # Validate transaction-date (if present)
    m_trx = DETAILS_TRXDATE_RE.search(details_raw)
    if m_trx:
        trx_date = _parse_date_ddmmyyyy(m_trx.group(1))
        if trx_date != primary_date:
            raise ValueError(
                f"Execution date mismatch: csv={primary_date} details={trx_date}"
            )
        details_rest = details_rest.replace(m_trx.group(1), "")

    normalized["primary_transaction_date"] = f"{primary_date} {hhmm}:00"

    # ----------------------------------------------------------------------
    # Remaining details → notes (for now) + debug column
    # ----------------------------------------------------------------------
    cleaned = re.sub(r"\s+", " ", details_rest).strip()
    normalized["notes"] = cleaned
    normalized["debug_details_rest"] = cleaned

    return normalized
