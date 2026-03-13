import re
from typing import Dict, Any, Optional


# ----------------------------------------------------------------------
# Regex definitions
# ----------------------------------------------------------------------

RE_BOOKING_DATE = re.compile(
    r"VALUTADATUM\s*:\s*(\d{2}/\d{2}/\d{4})$"
)

RE_EXTERNAL_ID = re.compile(
    r"BANKREFERENTIE\s*:\s*([0-9]+)"
)

RE_TRANSACTION_PROCESSING_DATE = re.compile(
    r"UITGEVOERD OP\s+(\d{2}/\d{2}(?:/\d{4})?)$"
)

RE_DESCRIPTION = re.compile(
    r"MEDEDELING\s*:\s*(.*)$"
)

RE_NO_DESCRIPTION = re.compile(
    r"\bZONDER\s+MEDEDELING\b$"
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

RE_PAYMENT_CHANNEL = re.compile(
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

RE_TRANSACTION_TYPE = re.compile(
    r"\b("
    r"UW\s+DOORLOPENDE\s+OPDRACHT\s+TEN\s+GUNSTE\s+VAN\s+REKENING|"
    r"WERO\s+OVERSCHRIJVING\s+IN\s+EURO|"
    r"INSTANTOVERSCHRIJVING\s+IN\s+EURO|"
    r"OVERSCHRIJVING\s+IN\s+EURO(?:\s+OP\s+REKENING|\s+VAN\s+REKENING)?|"
    r"EUROPESE\s+DOMICILIERING|"
    r"STORTING\s+VAN\s+[A-Z0-9 ,.\-]+|"
    r"TERUGBETALING\s+WOONKREDIET(?:\s+[0-9\-]+)?"
    r")\b"
)

RE_TRANSACTION_DIRECTION = re.compile(
    r"\b("
    r"OVERSCHRIJVING\s+IN\s+EURO\s+OP\s+REKENING|"
    r"OVERSCHRIJVING\s+IN\s+EURO\s+VAN\s+REKENING|"
    r"STORTING\s+VAN|"
    r"TERUGBETALING\s+WOONKREDIET"
    r")\b"
)

RE_TECHNICAL_REFERENCE = re.compile(
    r"\b("
    r"UW\s+REFERTE\s*:\s*[A-Z0-9\-]+|"
    r"REFERTE\s+OPDRACHTGEVER\s*:\s*[A-Z0-9\-]+|"
    r"REFERTE\s*:\s*[A-Z0-9\-]+"
    r")\b"
)

RE_ADDRESS = re.compile(
    r"\b\d{4}\s+[A-Z][A-Z\s\-]+(?:\s+[A-Z]{2,})?\b"
)

RE_STRUCTURED_REFERENCE = re.compile(
    r"(\+{0,3}\s*\d{3}\s*/\s*\d{4}\s*/\s*\d{5}\s*\+{0,3})"
)

RE_MANDATE_REFERENCE = re.compile(
    r"\bMANDAAT\s+NUMMER\s*:\s*([A-Z0-9]+)\b"
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

def extract_structured_message(value: str) -> Optional[str]:
    if not value:
        return None

    value = value.strip()

    # already formatted
    if re.fullmatch(r"\+{3}\d{3}/\d{4}/\d{5}\+{3}", value):
        return value

    # raw 12 digits
    if re.fullmatch(r"\d{12}", value):
        return canonicalize_structured_reference(value)

    return None

def normalize_for_message_compare(value: str) -> str:
    return re.sub(r"\s+", "", value).upper()


# ----------------------------------------------------------------------
# Main normalize function
# ----------------------------------------------------------------------

def normalize_row(csv_row: Dict[str, str]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {
        "external_id": "",                   # 1
        "primary_transaction_date": "",      # 2
        "transaction_processing_date": "",   # 3
        "booking_date": "",                  # 4
        "payment_date": "",                  # 5
        "amount": "",                        # 6
        "account_currency_code": "",         # 7
        "asset_account_iban": "",            # 8
        "opposing_account_iban": "",         # 9
        "opposing_account_bic": "",          # 10
        "opposing_account_name": "",         # 11
        "description": "",                   # 12
        "notes": "",                         # 13
    }


    # ------------------------------------------------------------------
    # A1–A8 — CSV context
    # ------------------------------------------------------------------

    normalized["amount"] = csv_row["amount"].replace(",", ".").strip()

    if csv_row["currency"] != "EUR":
        raise ValueError("Non-EUR account currency")
    normalized["account_currency_code"] = "EUR"

    normalized["asset_account_iban"] = csv_row["account_iban"].replace(" ", "").upper()

    execution_date = parse_ddmmyyyy(csv_row["execution_date"])
    normalized["primary_transaction_date"] = execution_date

    booking_date = parse_ddmmyyyy(csv_row["value_date"])
    normalized["booking_date"] = booking_date

    if not csv_row["transaction_type"]:
        raise ValueError("Missing transaction type")
    else:
        normalized["notes"] += f"\nTRANSACTION TYPE (CSV): {csv_row['transaction_type']}"


    csv_counterparty = csv_row.get("counterparty", "")
    if is_iban(csv_counterparty):
        normalized["opposing_account_iban"] = normalize_iban(csv_counterparty)

    normalized["opposing_account_name"] = csv_row.get("counterparty_name", "").strip()

    message = csv_row.get("message", "").strip()

    csv_structured = extract_structured_message(message)
    if csv_structured:
        normalized["description"] = csv_structured
    else:
        normalized["description"] = message

    # ------------------------------------------------------------------
    # DETAILS PIPELINE
    # ------------------------------------------------------------------

    details_rest = csv_row.get("details_raw", "")

    # B1 — Details - VALUTADATUM / booking_date
    match = RE_BOOKING_DATE.search(details_rest)
    if not match:
        raise ValueError("Missing VALUTADATUM in details")

    details_booking_date = parse_ddmmyyyy(match.group(1))
    if details_booking_date != normalized["booking_date"]:
        raise ValueError(
            "Value date mismatch between CSV and details: "
            f"csv_value_date='{normalized['booking_date']}' details_value_date='{details_booking_date}'"
        )

    details_rest = details_rest.replace(match.group(0), "").strip()

    # B2 — Details - BANKREFERENTIE / external_id
    match = RE_EXTERNAL_ID.search(details_rest)
    if not match:
        raise ValueError("Missing BANKREFERENTIE in details")

    normalized["external_id"] = match.group(1)
    details_rest = details_rest.replace(match.group(0), "").strip()

    # B3 — Details - UITGEVOERD OP / transaction_processing_date
    match = RE_TRANSACTION_PROCESSING_DATE.search(details_rest)
    if match:
        normalized["transaction_processing_date"] = parse_ddmmyyyy(
            match.group(1),
            fallback_year=execution_date[:4],
        )
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B4 — Details - MEDEDELING / description
    match = RE_DESCRIPTION.search(details_rest)
    if match:
        details_mededeling = match.group(1).strip()

        csv_description = normalized["description"]

        csv_structured = extract_structured_message(csv_description)
        details_structured = extract_structured_message(details_mededeling)

        # --- Structured message handling (authoritative, single value) ---
        if csv_structured or details_structured:
            if csv_structured and details_structured:
                if csv_structured != details_structured:
                    raise ValueError(
                        f"Structured message mismatch: "
                        f"csv='{csv_structured}' details='{details_structured}'"
                    )
                normalized["description"] = csv_structured
            elif csv_structured:
                normalized["description"] = csv_structured
            else:
                normalized["description"] = details_structured

        # --- Non-structured message handling (CSV wins, details validates) ---
        else:
            if csv_description:
                n_csv = normalize_for_message_compare(csv_description)
                n_det = normalize_for_message_compare(details_mededeling)

                if n_csv != n_det:
                    raise ValueError(
                        f"Mededeling mismatch: "
                        f"csv='{csv_description}' details='{details_mededeling}'"
                    )
                # CSV version is authoritative → keep it
            else:
                normalized["description"] = details_mededeling

        details_rest = details_rest.replace(match.group(0), "").strip()

    # B4a — Details - No message indicator
    match = RE_NO_DESCRIPTION.search(details_rest)
    if match:
        if normalized["description"]:
            raise ValueError("ZONDER MEDEDELING present but MEDEDELING already extracted")
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B5 — Details / payment_date
    match = RE_PAYMENT_DATE_TIME.search(details_rest)
    if match:
        payment_date = parse_ddmmyyyy(
            match.group(1),
            fallback_year=execution_date[:4],
        )
        payment_time = match.group(2) or "00:00"
        normalized["payment_date"] = f"{payment_date} {payment_time}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B6 — Details / notes - Card Network
    match = RE_CARD_NETWORK.search(details_rest)
    if match:
        normalized["notes"] += f"\n{match.group(1)}"
        if match.group(2):
            normalized["notes"] += f" ({match.group(2)})"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B7 — Details / notes - Card Number Container
    match = RE_CARD_NUMBER_CONTAINER.search(details_rest)
    if match:
        normalized["notes"] += f"\n{match.group(0).strip()}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B8 — Details / notes - Payment Channel
    match = RE_PAYMENT_CHANNEL.search(details_rest)
    if match:
        normalized["notes"] += f"\n{match.group(1)}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B9 — Details / notes - Transaction description
    match = RE_TRANSACTION_TYPE.search(details_rest)
    if match:
        transaction_description = match.group(1).strip()
        normalized["notes"] += f"\nTRANSACTION TYPE (DETAILS): {transaction_description}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B9a — Details / notes - Transaction direction
    match = RE_TRANSACTION_DIRECTION.search(details_rest)
    if match:
        transaction_direction = match.group(1).strip()
        normalized["notes"] += f"\nTRANSACTION DIRECTION: {transaction_direction}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B9b — Details / notes - Technical references
    match = RE_TECHNICAL_REFERENCE.search(details_rest)
    if match:
        technical_reference = match.group(1).strip()
        normalized["notes"] += f"\nTECHNICAL REFERENCE: {technical_reference}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B9c — Details / notes - Mandate reference
    match = RE_MANDATE_REFERENCE.search(details_rest)
    if match:
        mandate_reference = match.group(1).strip()
        normalized["notes"] += f"\nMANDATE REFERENCE: {mandate_reference}"
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B10 — Details - IBAN BIC / opposing_account_iban ; opposing_account_bic
    match = RE_IBAN_BIC.search(details_rest)
    if match:
        iban_norm = normalize_iban(match.group(1))
        bic_raw = match.group(2)

        if not is_iban(iban_norm):
            raise ValueError(f"Invalid IBAN in details: '{match.group(1)}' -> '{iban_norm}'")

        if normalized["opposing_account_iban"] and iban_norm != normalized["opposing_account_iban"]:
            raise ValueError(
                "IBAN mismatch between CSV and details: "
                f"csv_iban='{normalized['opposing_account_iban']}' details_iban='{iban_norm}'"
            )

        normalized["opposing_account_iban"] = iban_norm
        normalized["opposing_account_bic"] = bic_raw
        details_rest = details_rest.replace(match.group(0), "").strip()

    # B11 — Details / opposing_account_name
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
                raise ValueError(f"Ambiguous counterparty name: '{csv_name}' vs '{details_name}'")
        else:
            final_name = details_name
    else:
        final_name = csv_name

    if address_part:
        final_name = f"{final_name} {address_part}".strip()

    normalized["opposing_account_name"] = final_name

    # consume rest (B11 is "rest na knippen")
    details_rest = ""

    # ------------------------------------------------------------------
    # REST CHECK (FASE C)
    # ------------------------------------------------------------------
    if details_rest:
        raise ValueError(f"Unprocessed details content: {details_rest}")

    return normalized
