import re
import unicodedata
from datetime import date
from typing import Any

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def parse_iban(value: str, *, error_message: str | None = None) -> str:
    # Accept empty string as-is
    if value == "":
        return ""

    iban = re.sub(r"\s+", "", value).upper()

    if not re.fullmatch(r"[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}", iban):
        msg = error_message or f"Invalid IBAN: '{value}' -> '{iban}'"
        raise ValueError(msg)

    return iban


def parse_ddmmyyyy(
    value: str,
    fallback_date_str: str | None = None,
) -> str:
    """
    Parse 'dd/mm' or 'dd/mm/yyyy' into 'YYYY-MM-DD'.
    """

    # Convert fallback_date_str string → date object (into a NEW variable)
    if fallback_date_str is not None:
        fallback_date = date.fromisoformat(fallback_date_str)

    parts = value.split("/")
    if len(parts) == 3:
        day, month, year = map(int, parts)
        return date(year, month, day).isoformat()

    if len(parts) == 2:
        if fallback_date is None:
            raise ValueError(f"Missing year in date and no fallback_date provided: {value}")

        day, month = map(int, parts)

        candidates = []
        for year in (fallback_date.year - 1, fallback_date.year, fallback_date.year + 1):
            try:
                candidates.append(date(year, month, day))
            except ValueError:
                pass

        if not candidates:
            raise ValueError(f"Could not construct a valid date from: {value}")

        final_date = min(candidates, key=lambda d: abs(d - fallback_date))
        return final_date.isoformat()

    raise ValueError(f"Invalid date format: {value}")


def canonicalize_structured_ref(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) != 12:
        raise ValueError("Invalid structured reference")
    return f"+++{digits[0:3]}/{digits[3:7]}/{digits[7:12]}+++"


def extract_structured_ref(value: str | None) -> str | None:
    if not value:
        return None

    value = value.strip()

    # already formatted
    if re.fullmatch(r"\+{3}\d{3}/\d{4}/\d{5}\+{3}", value):
        return value

    # raw 12 digits
    if re.fullmatch(r"\d{12}", value):
        return canonicalize_structured_ref(value)

    return None


def normalize_for_comparison(value: str) -> str:
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", "", value).upper()


def append_note_line(notes: str, step: str, role: str, source: str, value: str) -> str:
    line = f"{step}) {role} ({source}): {value}"
    return f"{notes}\n{line}" if notes else line


# ----------------------------------------------------------------------
# Main normalize function
# ----------------------------------------------------------------------


def normalize_row(csv_row: dict[str, str]) -> dict[str, Any]:

    # ==================================================================
    # PHASE 1 — EXTRACTION
    # Pull all values out of the raw inputs into named variables.
    # No decisions about the output schema here.
    #
    # details is parsed sequentially and destructively: each matched
    # segment is removed from remaining_details so later patterns cannot match
    # already-extracted content. The order is determined by parsing
    # reliability, not by the output schema.
    # ==================================================================

    # -- Single purpose columns --
    column_external_id = csv_row["external_id"]
    column_amount = csv_row["amount"]
    column_account_currency_code = csv_row["account_currency_code"]
    column_asset_account_iban = csv_row["asset_account_iban"]
    raw_opposing_account_iban = csv_row.get("opposing_account_iban", "").strip()
    column_opposing_account_iban = parse_iban(
        raw_opposing_account_iban, error_message=f"Invalid IBAN in tegenpartij column: '{raw_opposing_account_iban}'"
    )
    column_opposing_account_name = csv_row.get("opposing_account_name", "").strip()
    column_description = csv_row.get("description", "").strip()
    column_transaction_type = csv_row["transaction_type"]
    column_primary_transaction_date = parse_ddmmyyyy(csv_row["primary_transaction_date"])
    column_booking_date = parse_ddmmyyyy(csv_row["booking_date"])

    # -- Multi purpose column: details --
    remaining_details = csv_row.get("details", "")

    # B1 — Details - Booking date = mandatory
    RE_BOOKING_DATE = re.compile(r"VALUTADATUM\s*:\s*(\d{2}/\d{2}/\d{4})$")
    match = RE_BOOKING_DATE.search(remaining_details)
    if not match:
        raise ValueError("Missing VALUTADATUM in details")
    details_booking_date = parse_ddmmyyyy(match.group(1))
    remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B2 — Details - Bank reference = optional
    details_bank_reference = None
    RE_BANK_REFERENCE = re.compile(r"BANKREFERENTIE\s*:\s*([0-9]+)")
    match = RE_BANK_REFERENCE.search(remaining_details)
    if match:
        details_bank_reference = match.group(1)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B3 — Details - Transaction processing date = optional
    details_transaction_processing_date = None
    RE_TRANSACTION_PROCESSING_DATE = re.compile(r"UITGEVOERD OP\s+(\d{2}/\d{2}(?:/\d{4})?)$")
    match = RE_TRANSACTION_PROCESSING_DATE.search(remaining_details)
    if match:
        details_transaction_processing_date = parse_ddmmyyyy(
            match.group(1), fallback_date_str=column_primary_transaction_date
        )
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B4 — Details - Description = optional
    details_description = None
    RE_DESCRIPTION = re.compile(r"MEDEDELING\s*:\s*(.*)$")
    match = RE_DESCRIPTION.search(remaining_details)
    if match:
        details_description = match.group(1).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B4a — Details - No description = optional
    details_no_description = False
    RE_NO_DESCRIPTION = re.compile(r"\bZONDER\s+MEDEDELING\b$")
    match = RE_NO_DESCRIPTION.search(remaining_details)
    if match:
        details_no_description = True
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B5 — Details - Payment date = optional
    details_payment_date = None
    RE_PAYMENT_DATE = re.compile(r"(\d{2}/\d{2}/\d{4})(?:\s+(\d{2}:\d{2}))?")
    match = RE_PAYMENT_DATE.search(remaining_details)
    if match:
        details_payment_date = (
            parse_ddmmyyyy(match.group(1), fallback_date_str=column_primary_transaction_date)
            + " "
            + (match.group(2) or "00:00")
        )
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B6 — Details - Card network = optional
    details_card_network = None
    RE_CARD_NETWORK = re.compile(
        r"(BANCONTACT(?: PAYCONIQ CO)?|VISA DEBIT)"
        r"(?:\s*-\s*(CONTACTLOOS|eCommerce))?"
    )
    match = RE_CARD_NETWORK.search(remaining_details)
    if match:
        details_card_network = match.group(1)
        if match.group(2):
            details_card_network += f" ({match.group(2)})"
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B8 — Details - Payment channel = optional
    details_payment_channel = None
    RE_PAYMENT_CHANNEL = re.compile(r"(VIA MOBILE BANKING|VIA WEB BANKING|P2P MOBILE|MOBIELE BETALING)")
    match = RE_PAYMENT_CHANNEL.search(remaining_details)
    if match:
        details_payment_channel = match.group(1)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B7 — Details - Card container = optional
    details_card_container = None
    RE_CARD_NUMBER_CONTAINER = re.compile(
        r"(BETALING MET DEBETKAART NUMMER|"
        r"OP DE REKENING GEKOPPELD AAN DE DEBETKAART NUMMER)\s+([0-9X ]+)"
    )
    match = RE_CARD_NUMBER_CONTAINER.search(remaining_details)
    if match:
        details_card_container = match.group(0).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B9 — Details - Transaction type = optional
    details_transaction_type = None
    details_description_override = None  # MAANDELIJKSE BIJDRAGE tail → replaces description
    RE_TRANSACTION_TYPE = re.compile(
        r"\b(?:"
        r"(STORTING)(?:\s+VAN\s+(.+))?"  # (1)=details_transaction_type, (2)=det_transaction_type_storting_van_tail
        r"|"
        r"(MAANDELIJKSE\s+BIJDRAGE)(?:\s+(.+))?"  # (3)=details_transaction_type, (4)=det_transaction_type_tail
        r"|"
        r"("
        r"UW\s+DOORLOPENDE\s+OPDRACHT\s+TEN\s+GUNSTE\s+VAN\s+REKENING|"
        r"WERO\s+OVERSCHRIJVING\s+IN\s+EURO|"
        r"INSTANTOVERSCHRIJVING\s+IN\s+EURO|"
        r"OVERSCHRIJVING\s+IN\s+EURO(?:\s+OP\s+REKENING|\s+VAN\s+REKENING)?|"
        r"EUROPESE\s+DOMICILIERING|"
        r"TERUGBETALING\s+WOONKREDIET(?:\s+[0-9\-]+)?"
        r")"  # (5)=type
        r")\b",
        re.IGNORECASE,
    )
    match = RE_TRANSACTION_TYPE.search(remaining_details)
    if match:
        details_transaction_type = (match.group(1) or match.group(3) or match.group(5)).strip()
        det_transaction_type_storting_van_tail = match.group(2).strip() if match.group(2) else None
        det_transaction_type_tail = match.group(4).strip() if match.group(4) else None

        if details_transaction_type.upper() == "STORTING" and det_transaction_type_storting_van_tail:
            remaining_details = (
                det_transaction_type_storting_van_tail  # remainder becomes Opposing account name info for B11
            )
        elif details_transaction_type.upper() == "MAANDELIJKSE BIJDRAGE" and det_transaction_type_tail:
            details_description_override = det_transaction_type_tail
            remaining_details = ""  # nothing further to extract
        else:
            remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B9a — Details - Transaction direction = removed (derived from amount)
    RE_TRANSACTION_DIRECTION = re.compile(
        r"\b("
        r"OVERSCHRIJVING\s+IN\s+EURO\s+OP\s+REKENING|"
        r"OVERSCHRIJVING\s+IN\s+EURO\s+VAN\s+REKENING|"
        r"OPDRACHTGEVER\s+REKENING\s*:"
        r")",
        re.IGNORECASE,
    )
    match = RE_TRANSACTION_DIRECTION.search(remaining_details)
    if match:
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B9b — Details - Technical reference = optional
    details_technical_reference = None
    RE_TECHNICAL_REFERENCE = re.compile(
        r"\b("
        r"UW\s+REFERTE\s*:\s*.+?|"
        r"REFERTE\s+OPDRACHTGEVER\s*:\s*.+?|"
        r"REFERTE\s*:\s*.+?"
        r")(?=\s+(?:MANDAAT\s+NUMMER)\s*:|$)",
        re.IGNORECASE,
    )
    match = RE_TECHNICAL_REFERENCE.search(remaining_details)
    if match:
        details_technical_reference = match.group(1).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B9c — Details - mandate reference = optional
    details_mandate_reference = None
    RE_MANDATE_REFERENCE = re.compile(r"\bMANDAAT\s+NUMMER\s*:\s*([A-Z0-9]+)\b")
    match = RE_MANDATE_REFERENCE.search(remaining_details)
    if match:
        details_mandate_reference = match.group(1).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B10 — Details - IBAN + BIC = optional
    details_opposing_account_iban = None
    details_opposing_account_bic = None
    RE_IBAN_BIC = re.compile(
        r"\b([A-Z]{2}\s*\d{2}(?:\s*[A-Z0-9]){11,30})\s+BIC\s+([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b",
        re.IGNORECASE,
    )
    match = RE_IBAN_BIC.search(remaining_details)
    if match:
        details_opposing_account_iban = parse_iban(match.group(1))
        details_opposing_account_bic = match.group(2)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # B11 — Details - Opposing account name / address = optional
    details_opposing_account_name = remaining_details.strip()

    # ==================================================================
    # PHASE 2 — WRITE
    # Map extracted values to normalized output fields.
    # All cross-source reconciliation and suppression logic lives here.
    # No regex or string parsing; only decisions.
    # ==================================================================

    normalized: dict[str, Any] = {
        "external_id": "",  # 1
        "primary_transaction_date": "",  # 2
        "transaction_processing_date": "",  # 3
        "booking_date": "",  # 4
        "payment_date": "",  # 5
        "amount": "",  # 6
        "account_currency_code": "",  # 7
        "asset_account_iban": "",  # 8
        "opposing_account_iban": "",  # 9
        "opposing_account_bic": "",  # 10
        "opposing_account_name": "",  # 11
        "description": "",  # 12
        "notes": "",  # 13
    }

    # external_id ← column_external_id
    normalized["external_id"] = column_external_id

    # primary_transaction_date ← column_primary_transaction_date
    normalized["primary_transaction_date"] = column_primary_transaction_date

    # transaction_processing_date ← details_transaction_processing_date (optional)
    normalized["transaction_processing_date"] = details_transaction_processing_date or ""

    # booking_date ← column_booking_date &| details_booking_date (match)
    if details_booking_date != column_booking_date:
        raise ValueError(
            "Value date mismatch between dedicated column and details: "
            f"column_booking_date='{column_booking_date}' details_booking_date='{details_booking_date}'"
        )
    normalized["booking_date"] = column_booking_date

    # payment_date ← details_payment_date (optional)
    normalized["payment_date"] = details_payment_date or ""

    # amount ← column_amount
    normalized["amount"] = column_amount.replace(",", ".").strip()

    # account_currency_code ← column_account_currency_code
    if column_account_currency_code != "EUR":
        raise ValueError("Non-EUR account currency")
    normalized["account_currency_code"] = column_account_currency_code

    # asset_account_iban ← column_asset_account_iban
    normalized["asset_account_iban"] = column_asset_account_iban.replace(" ", "").upper()

    # opposing_account_iban ← details_opposing_account_iban &| details_opposing_account_iban (optional match)
    if (
        column_opposing_account_iban
        and details_opposing_account_iban
        and column_opposing_account_iban != details_opposing_account_iban
    ):
        raise ValueError(
            "IBAN mismatch between CSV and details: "
            f"column_opposing_account_iban='{column_opposing_account_iban}' "
            f"details_opposing_account_iban='{details_opposing_account_iban}'"
        )
    normalized["opposing_account_iban"] = column_opposing_account_iban or details_opposing_account_iban or ""

    # opposing_account_bic ← details_opposing_account_bic
    normalized["opposing_account_bic"] = details_opposing_account_bic or ""

    # opposing_account_name ← column_opposing_account_name &| details_opposing_account_name (append)
    if column_opposing_account_name:
        if details_opposing_account_name:
            column_opposing_account_name_norm = normalize_for_comparison(column_opposing_account_name)
            details_opposing_account_name_norm = normalize_for_comparison(details_opposing_account_name)
            if column_opposing_account_name_norm not in details_opposing_account_name_norm:
                raise ValueError(
                    "Opposing account name mismatch: "
                    f"column='{column_opposing_account_name}' "
                    f"details='{details_opposing_account_name}'"
                )
            if details_opposing_account_name_norm.startswith(column_opposing_account_name_norm):
                tail_norm_len = len(details_opposing_account_name_norm) - len(column_opposing_account_name_norm)
                if tail_norm_len > 0:
                    consumed = 0
                    cut_index = 0
                    for index, char in enumerate(details_opposing_account_name):
                        if not char.isspace():
                            consumed += 1
                        if consumed >= len(column_opposing_account_name_norm):
                            cut_index = index + 1
                            break
                    details_opposing_account_name_tail = details_opposing_account_name[cut_index:].strip()
                    normalized["opposing_account_name"] = (
                        f"{column_opposing_account_name} {details_opposing_account_name_tail}".strip()
                        if details_opposing_account_name_tail
                        else column_opposing_account_name
                    )
                else:
                    normalized["opposing_account_name"] = column_opposing_account_name
            else:
                # details has extra leading words ("VAN ...") → keep CSV only (no safe splice)
                normalized["opposing_account_name"] = column_opposing_account_name
        else:
            normalized["opposing_account_name"] = column_opposing_account_name
    else:
        normalized["opposing_account_name"] = details_opposing_account_name

    # description ← details_description_override |
    #               (column_description &| details_description &| column_structured_ref &| details_structured_ref)
    # MAANDELIJKSE BIJDRAGE free tail overrides both when present
    if details_description_override is not None:
        normalized["description"] = details_description_override
    else:
        if details_no_description and (column_description or details_description):
            raise ValueError("ZONDER MEDEDELING present but description found in a dedicated column or details column")

        column_structured_ref = extract_structured_ref(column_description)
        details_structured_ref = extract_structured_ref(details_description)

        if column_structured_ref or details_structured_ref:
            if column_structured_ref and details_structured_ref and column_structured_ref != details_structured_ref:
                raise ValueError(
                    f"Structured reference mismatch: csv='{column_structured_ref}' details='{details_structured_ref}'"
                )
            normalized["description"] = column_structured_ref or details_structured_ref
        elif column_description:
            if details_description:
                if normalize_for_comparison(column_description) != normalize_for_comparison(details_description):
                    raise ValueError(
                        f"Mededeling mismatch: column='{column_description}' details='{details_description}'"
                    )
            normalized["description"] = column_description
        else:
            normalized["description"] = details_description or ""

    # notes ← assembled from multiple sources
    #
    # Normalize transaction type strings once; used by B9, A5, and B8 decisions.
    if not column_transaction_type:
        raise ValueError("Missing transaction type")
    column_transaction_type_norm = normalize_for_comparison(column_transaction_type)

    details_transaction_type_norm = None
    if details_transaction_type:
        details_transaction_type_norm = normalize_for_comparison(details_transaction_type)
        for suffix in ("VAN REKENING", "NAAR REKENING", "OP REKENING"):
            suffix_norm = normalize_for_comparison(suffix)
            if details_transaction_type_norm.endswith(suffix_norm):
                details_transaction_type_norm = details_transaction_type_norm[: -len(suffix_norm)]
                break

    notes = ""

    # B2 — details_bank_reference
    if details_bank_reference:
        notes = append_note_line(notes, "B2", "BANK REFERENCE", "details", details_bank_reference)

    # B6 / B7 — details_card_network, details_payment_channel, details_card_container
    # When a details_card_container was found, details_card_network + details_payment_channel are merged into B7
    # When no details_card_container,
    # details_card_network goes to B6 and details_payment_channel is available for B8/A5.
    if details_card_container:
        card_parts = [p for p in [details_card_network, details_payment_channel, details_card_container] if p]
        notes = append_note_line(notes, "B7", "CARD IDENTIFIER", "details", " ".join(card_parts))
    elif details_card_network:
        notes = append_note_line(notes, "B6", "CARD NETWORK", "details", details_card_network)

    # B9 — transaction type from details (only when it adds info beyond CSV tx type)
    if details_transaction_type and details_transaction_type_norm != column_transaction_type_norm:
        notes = append_note_line(notes, "B9", "TRANSACTION TYPE", "details", details_transaction_type)

    # B9b — details_technical_reference
    if details_technical_reference:
        notes = append_note_line(notes, "B9b", "TECHNICAL REFERENCE", "details", details_technical_reference)

    # B9c — details_mandate_reference
    if details_mandate_reference:
        notes = append_note_line(notes, "B9c", "MANDATE REFERENCE", "details", details_mandate_reference)

    # A5 — col_transaction_type_det_payment_channel
    # Suppressed when: (1) card payment with debetkaart, or (2) B9 is a more specific form of A5.
    # Payment channel appended to A5 when channel is present, no card container, and channel starts with VIA.
    details_norm = normalize_for_comparison(csv_row.get("details", ""))
    suppress_col_transaction_type_det_payment_channel = (
        column_transaction_type_norm == "KAARTBETALING" and "DEBETKAART" in details_norm
    ) or (
        details_transaction_type_norm is not None
        and details_transaction_type_norm != column_transaction_type_norm
        and column_transaction_type_norm in details_transaction_type_norm
    )

    details_payment_channel_available = details_payment_channel is not None and details_card_container is None
    append_det_payment_channel_to_col_transaction_type_det_payment_channel = (
        details_payment_channel is not None
        and details_card_container is None
        and details_payment_channel.upper().startswith("VIA")
    )

    if not suppress_col_transaction_type_det_payment_channel:
        col_transaction_type_det_payment_channel = column_transaction_type
        if append_det_payment_channel_to_col_transaction_type_det_payment_channel:
            col_transaction_type_det_payment_channel = (
                f"{col_transaction_type_det_payment_channel} {details_payment_channel}"
            )
        notes = append_note_line(
            notes, "A5", "TRANSACTION TYPE", "transaction_type", col_transaction_type_det_payment_channel
        )

    # B8 — details_payment_channel (only when not already merged into B7 or A5)
    if details_payment_channel_available and not append_det_payment_channel_to_col_transaction_type_det_payment_channel:
        notes = append_note_line(notes, "B8", "PAYMENT CHANNEL", "details", details_payment_channel or "")

    normalized["notes"] = notes

    return normalized
