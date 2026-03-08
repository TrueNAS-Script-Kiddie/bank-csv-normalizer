import re
from typing import Dict, Any, Optional


# ----------------------------------------------------------------------
# Regex definitions
# ----------------------------------------------------------------------

RE_VALUE_DATE = re.compile(
    r"VALUTADATUM\s*:\s*(\d{2}/\d{2}/\d{4})$"
)

RE_BANK_REFERENCE = re.compile(
    r"BANKREFERENTIE\s*:\s*([0-9]+)"
)

RE_PROCESSED_ON_DATE = re.compile(
    r"UITGEVOERD OP\s+(\d{2}/\d{2}(?:/\d{4})?)$"
)

RE_PAYMENT_DATE_TIME = re.compile(
    r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}))?"
)

RE_CARD_NETWORK = re.compile(
    r"(BANCONTACT(?: PAYCONIQ CO)?|VISA DEBIT)"
    r"(?:\s*-\s*(CONTACTLOOS|eCommerce))?"
)

RE_CARD_NUMBER_CONTAINER = re.compile(
    r"(BETALING MET DEBETKAART NUMMER|"
    r"OP DE REKENING GEKOPPELD AAN DE DEBETKAART NUMMER)\s+([0-9X ]+)"
)

RE_CHANNEL = re.compile(
    r"(VIA MOBILE BANKING|VIA WEB BANKING|P2P MOBILE|MOBIELE BETALING)"
)

RE_IBAN_STRICT = re.compile(
    r"[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}"
)

RE_IBAN_BEFORE_BIC = re.compile(
    r"\b([A-Z]{2}\s*\d{2}(?:\s*[A-Z0-9]){10,30})\s+BIC\b"
)

RE_BIC = re.compile(
    r"\b([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b"
)

RE_IBAN_BIC = re.compile(
    r"\b([A-Z]{2}\s*\d{2}(?:\s*[A-Z0-9]){10,30})\s+BIC\s+([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b"
)

RE_ADDRESS = re.compile(
    r"\b\d{4}\s+[A-Z][A-Z\s\-]+(?:\s+[A-Z]{2,})?\b"
)

RE_STRUCTURED_REFERENCE = re.compile(
    r"(\+{0,3}\s*\d{3}\s*/\s*\d{4}\s*/\s*\d{5}\s*\+{0,3})"
)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def normalize_iban(value: str) -> str:
    return re.sub(r"\s+", "", value).upper()


def is_iban(value: str) -> bool:
    return bool(RE_IBAN_STRICT.fullmatch(normalize_iban(value)))


def parse_ddmmyyyy(value: str, fallback_year: Optional[str] = None) -> str:
    parts = value.split("/")
    if len(parts) == 3:
        day, month, year = parts
    elif len(parts) == 2:
        if not fallback_year:
            raise ValueError(f"Missing year in date: {value}")
        day, month = parts
        year = fallback_year
    else:
        raise ValueError(f"Invalid date format: {value}")

    return f"{year}-{month}-{day}"


def canonicalize_structured_reference(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) != 12:
        raise ValueError("Invalid structured reference")
    return f"+++{digits[0:3]}/{digits[3:7]}/{digits[7:12]}+++"


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.upper()).strip()


# ----------------------------------------------------------------------
# Main normalize function
# ----------------------------------------------------------------------

def normalize_row(csv_row: Dict[str, str]) -> Dict[str, Any]:

    normalized: Dict[str, Any] = {
        "amount": "",
        "account_currency_code": "",
        "asset_account_iban": "",
        "asset_account_bic": "",
        "booking_date": "",
        "primary_transaction_date": "",
        "transaction_processing_date": "",
        "external_id": "",
        "opposing_account_iban": "",
        "opposing_account_bic": "",
        "opposing_account_name": "",
        "description": "",
        "notes": "",
    }

    # ------------------------------------------------------------------
    # A1–A8 — CSV context
    # ------------------------------------------------------------------

    normalized["amount"] = csv_row["amount"].replace(",", ".").strip()

    if csv_row["currency"] != "EUR":
        raise ValueError("Non-EUR account currency")
    normalized["account_currency_code"] = "EUR"

    normalized["asset_account_iban"] = csv_row["account_iban"].replace(
        " ", ""
    ).upper()

    execution_date = parse_ddmmyyyy(csv_row["execution_date"])
    normalized["primary_transaction_date"] = execution_date

    booking_date = parse_ddmmyyyy(csv_row["value_date"])
    normalized["booking_date"] = booking_date

    if not csv_row["transaction_type"]:
        raise ValueError("Missing transaction type")

    csv_counterparty = csv_row.get("counterparty", "")
    if is_iban(csv_counterparty):
        normalized["opposing_account_iban"] = normalize_iban(csv_counterparty)

    normalized["opposing_account_name"] = csv_row.get(
        "counterparty_name", ""
    ).strip()

    message = csv_row.get("message", "").strip()
    structured_match = RE_STRUCTURED_REFERENCE.search(message)
    if structured_match:
        normalized["notes"] = canonicalize_structured_reference(
            structured_match.group(1)
        )
    else:
        normalized["description"] = message

    # ------------------------------------------------------------------
    # DETAILS PIPELINE
    # ------------------------------------------------------------------

    details_rest = csv_row.get("details_raw", "")

    # B1 — VALUE DATE
    match = RE_VALUE_DATE.search(details_rest)
    if not match:
        raise ValueError("Missing value date in details")

    if parse_ddmmyyyy(match.group(1)) != booking_date:
        details_value_date_raw = match.group(1)
        details_value_date = parse_ddmmyyyy(details_value_date_raw)

        raise ValueError(
            "Value date mismatch between CSV and details: "
            f"csv_value_date='{booking_date}' "
            f"details_value_date_raw='{details_value_date_raw}' "
            f"details_value_date='{details_value_date}'"
        )

    details_rest = details_rest.replace(match.group(0), "").strip()

    # B2 — BANK REFERENCE
    match = RE_BANK_REFERENCE.search(details_rest)
    if not match:
        raise ValueError("Missing bank reference")

    normalized["external_id"] = match.group(1)
    details_rest = details_rest.replace(match.group(0), "").strip()

    # B3 — PROCESSED ON DATE
    match = RE_PROCESSED_ON_DATE.search(details_rest)
    if match:
        normalized["transaction_processing_date"] = parse_ddmmyyyy(
            match.group(1),
            fallback_year=execution_date[:4],
        )
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B4 — PAYMENT DATETIME (aankoopmoment; altijd datetime)
    match = RE_PAYMENT_DATE_TIME.search(details_rest)
    if match:
        payment_date = parse_ddmmyyyy(
            match.group(1),
            fallback_year=execution_date[:4],
        )
        payment_time = match.group(2) or "00:00"

        normalized["payment_date"] = f"{payment_date} {payment_time}"

        details_rest = details_rest.replace(match.group(0), "").strip()

    # B8 — DETAILS-TYPE / SUBTYPE (notes)
    match = RE_CARD_NETWORK.search(details_rest)
    if match:
        normalized["notes"] += f" | {match.group(1)}"
        if match.group(2):
            normalized["notes"] += f" ({match.group(2)})"
        details_rest = details_rest.replace(match.group(0), "").strip()

    match = RE_CARD_NUMBER_CONTAINER.search(details_rest)
    if match:
        normalized["notes"] += f" | Card {match.group(2).strip()}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    match = RE_CHANNEL.search(details_rest)
    if match:
        normalized["notes"] += f" | {match.group(1)}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B9 — DETAILS-IBAN / BIC (atomic extraction)
    match = RE_IBAN_BIC.search(details_rest)
    if match:
        iban_raw = match.group(1)
        bic_raw = match.group(2)

        iban_norm = normalize_iban(iban_raw)

        if not is_iban(iban_norm):
            raise ValueError(
                f"Invalid IBAN before BIC: raw='{iban_raw}' norm='{iban_norm}'"
            )

        if (
            normalized["opposing_account_iban"]
            and iban_norm != normalized["opposing_account_iban"]
        ):
            raise ValueError(
                "IBAN mismatch between CSV and details: "
                f"csv_iban='{normalized['opposing_account_iban']}' "
                f"details_iban='{iban_norm}'"
            )

        normalized["opposing_account_iban"] = iban_norm
        normalized["opposing_account_bic"] = bic_raw

        # pas nu destructief verwijderen
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B10 — MERCHANT / TEGENPARTIJ (rest na knippen)
    address_part = ""
    match = RE_ADDRESS.search(details_rest)
    if match:
        address_part = match.group(0).strip()
        details_rest = details_rest.replace(match.group(0), "").strip()

    details_name = details_rest.strip()
    csv_name = normalized["opposing_account_name"]

    if details_name:
        if csv_name:
            n_csv = normalize_name(csv_name)
            n_det = normalize_name(details_name)

            if n_csv in n_det:
                final_name = details_name
            elif n_det in n_csv:
                final_name = csv_name
            else:
                raise ValueError(
                    f"Ambiguous counterparty name: "
                    f"'{csv_name}' vs '{details_name}'"
                )
        else:
            final_name = details_name
    else:
        final_name = csv_name

    if address_part:
        final_name = f"{final_name} {address_part}".strip()

    normalized["opposing_account_name"] = final_name
    details_rest = ""

    # ------------------------------------------------------------------
    # REST CHECK
    # ------------------------------------------------------------------
    if details_rest:
        raise ValueError(f"Unprocessed details content: {details_rest}")

    return normalized
