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

    fallback_date = None
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


def append_note_line(notes: str, source: str, value: str) -> str:
    line = f"{source}: {value}"
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

    details_booking_date = None
    details_bank_reference = None
    details_transaction_processing_date = None
    details_description = None
    details_opposing_account_name = None
    details_transaction_type = None
    details_payment_date = None
    details_opposing_account_iban = None
    details_opposing_account_bic = None
    details_technical_reference = None
    details_exchange_and_transaction_costs = None

    details_match_type = None

    # A1 — Postfix: VALUTADATUM
    RE_BOOKING_DATE = re.compile(r"VALUTADATUM\s*:\s*(\d{2}/\d{2}/\d{4})$")
    match = RE_BOOKING_DATE.search(remaining_details)
    if match:
        details_booking_date = parse_ddmmyyyy(match.group(1))
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A2 — Postfix: BANKREFERENTIE
    RE_BANK_REFERENCE = re.compile(r"BANKREFERENTIE\s*:\s*([0-9]+)$")
    match = RE_BANK_REFERENCE.search(remaining_details)
    if match:
        details_bank_reference = match.group(1)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A3 — Postfix: UITGEVOERD OP
    RE_TRANSACTION_PROCESSING_DATE = re.compile(r"UITGEVOERD OP\s+(\d{2}/\d{2}(?:/\d{4})?)$")
    match = RE_TRANSACTION_PROCESSING_DATE.search(remaining_details)
    if match:
        details_transaction_processing_date = parse_ddmmyyyy(
            match.group(1), fallback_date_str=column_primary_transaction_date
        )
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A4 — details_description
    RE_DESCRIPTION = re.compile(
        r"MEDEDELING\s*:\s*(.*)$"  # group 1: details_description
        r"|"
        r"^(TERUGBETALING\s+WOONKREDIET(?:\s+[0-9\-]+)?)"  # group 2: details_description
        r"|"
        r"^(MAANDELIJKSE\s+BIJDRAGE(?:\s+.+)?)"  # group 3: details_description
        r"|"
        r"^(UITBETALING VAN UW BONUS [0-9]{4})(?: UW TROUW WORDT BELOOND ZIE BIJLAGE VOOR DETAILS)"
        # group 4: details_description
        r"|"
        r"^(NETTO INTERESTEN)(?:\s?\:\s?DETAILS ZIE BIJLAGE)?"  # group 5: details_description
        r"|"
        r"^((?:MAANDELIJKSE )?EQUIPERINGSKOSTEN VOOR DE PERIODE .+?)(?:\s+DETAILS ZIE BIJLAGE)?$"
        # group 6: fee description
        r"|"
        r"^(GEBRUIKSKOSTEN VOOR DE PERIODE .+?)(?:\s+DETAILS ZIE BIJLAGE)?$"  # group 7: fee description
        r"|"
        r"^(INSCHRIJVING OP BELGISCHE EFFECTEN REFERTE : [0-9]+)$"  # group 8: pensioensparen
        r"|"
        r"^((?:INTEREST|DOSSIERKOSTEN VOOR|VERVROEGDE TERUGBETALING) KREDIET( [0-9\-]+)?)$"  # group 9: krediet
    )
    match = RE_DESCRIPTION.search(remaining_details)
    if match:
        details_match_type = "Description"
        details_description = (
            match.group(1)
            or match.group(2)
            or match.group(3)
            or match.group(4)
            or match.group(5)
            or match.group(6)
            or match.group(7)
            or match.group(8)
            or match.group(9)
        ).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A4a — details_no_description
    details_no_description = False
    RE_NO_DESCRIPTION = re.compile(r"\bZONDER\s+MEDEDELING\b$")
    match = RE_NO_DESCRIPTION.search(remaining_details)
    if match:
        details_match_type = "No description"
        details_no_description = True
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A5 — STORTING
    RE_STORTING = re.compile(
        r"^(STORTING)"  # group 1: details_transaction_type
        r"( VAN )"  # group 2: drop
        r"(.+?)"  # group 3: details_opposing_account_name
        r"( OP DE REKENING GEKOPPELD AAN DE DEBETKAART NUMMER )"  # group 4: details_transaction_type
        r"([0-9X]{4}(?:\s[0-9X]{4}){3})"  # group 5: details_transaction_type
        r"( [0-9]{2}/[0-9]{2}/[0-9]{4})"  # group 6: details_payment_date
    )
    match = RE_STORTING.search(remaining_details)
    if match:
        details_match_type = "Storting"
        details_opposing_account_name = match.group(3).strip()
        details_payment_date = match.group(6).strip()
        details_transaction_type = match.group(1) + match.group(4) + match.group(5)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A6 — DOORLOPENDE OPDRACHT
    RE_DOORLOPENDE_OPDRACHT = re.compile(
        r"^(UW )"  # group 1: drop
        r"(DOORLOPENDE OPDRACHT)"  # group 2: details_transaction_type
        r"( TEN GUNSTE VAN REKENING )"  # group 3: drop
        r"(.+)$"  # group 4: details_opposing_account_iban/bic/name
    )
    match = RE_DOORLOPENDE_OPDRACHT.search(remaining_details)
    if match:
        details_match_type = "Doorlopende opdracht"
        details_transaction_type = match.group(2).strip()
        rest = match.group(4).strip()
        RE_IBAN_BIC = re.compile(
            r"\b([A-Z]{2}\s*\d{2}(?:\s*[A-Z0-9]){11,30})\s+BIC\s+([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b",
            re.IGNORECASE,
        )
        match_iban_bic = RE_IBAN_BIC.search(rest)
        if match_iban_bic:
            details_opposing_account_iban = parse_iban(match_iban_bic.group(1))
            details_opposing_account_bic = match_iban_bic.group(2)
            rest = rest.replace(match_iban_bic.group(0), "").strip()
        details_opposing_account_name = rest.strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A7 — DOMICILIERING
    RE_DOMICILIERING = re.compile(
        r"^(?:(EERSTE INVORDERING VAN EEN ))?"  # group 1: details_transaction_type optional prefix
        r"((?:GEWEIGERDE )?EUROPESE DOMICILIERING)"  # group 2: details_transaction_type core
        r"( VAN )"  # group 3: drop
        r"(.+?)"  # group 4: details_opposing_account_name
        r"( MANDAAT NUMMER : )"  # group 5: details_technical_reference fixed
        r"([A-Z0-9\-]+)"  # group 6: details_technical_reference mandate number
        r"( REFERTE : )"  # group 7: details_technical_reference fixed
        r"(.*)$"  # group 8: details_technical_reference reference
    )
    match = RE_DOMICILIERING.search(remaining_details)
    if match:
        details_match_type = "Domiciliering"
        details_transaction_type = ((match.group(1) or "") + match.group(2)).strip()
        details_opposing_account_name = match.group(4).strip()
        details_technical_reference = (match.group(5) + match.group(6) + match.group(7) + match.group(8)).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A8 — OVERSCHRIJVING
    RE_OVERSCHRIJVING = re.compile(
        r"^"
        r"(WERO |INSTANT|EUROPESE |INSTANT EUROPESE )?"  # group 1: details_transaction_type prefix
        r"(OVERSCHRIJVING)"  # group 2: details_transaction_type core
        r"( IN EURO)?"  # group 3: drop
        r"( OP REKENING| VAN REKENING| NAAR)?"  # group 4: drop
        r"(.*?)(?= VIA WEB BANKING| VIA MOBILE BANKING| REFERTE OPDRACHTGEVER| UW REFERTE|$)"
        # group 5: details_opposing_account_iban/bic/name (part 1)
        r"( VIA WEB BANKING| VIA MOBILE BANKING)?"  # group 6: details_transaction_type via
        r"(.*?)(?= REFERTE OPDRACHTGEVER| UW REFERTE|$)"
        # group 7: details_opposing_account_iban/bic/name (part 2)
        r"(?:( REFERTE OPDRACHTGEVER| UW REFERTE)"  # group 8: details_technical_reference label
        r"( : )"  # group 9: drop
        r"(.*?))?$",  # group 10: details_technical_reference value
        re.IGNORECASE,
    )
    match = RE_OVERSCHRIJVING.search(remaining_details)
    if match:
        details_match_type = "Overschrijving"
        # details_transaction_type
        prefix = (match.group(1) or "").strip()
        core = match.group(2).strip()
        via = (match.group(6) or "").strip()
        details_transaction_type = " ".join(x for x in [prefix, core, via] if x).strip()
        # details_technical_reference
        if match.group(8) and match.group(10):
            details_technical_reference = match.group(8).strip() + " : " + match.group(10).strip()
        # details_opposing_account_iban/bic/name
        rest = (match.group(5) or "") + " " + (match.group(7) or "").strip()
        RE_IBAN_BIC = re.compile(
            r"\b([A-Z]{2}\s*\d{2}(?:\s*[A-Z0-9]){11,30})\s+BIC\s+([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b",
            re.IGNORECASE,
        )
        match_iban_bic = RE_IBAN_BIC.search(rest)
        if match_iban_bic:
            details_opposing_account_iban = parse_iban(match_iban_bic.group(1))
            details_opposing_account_bic = match_iban_bic.group(2)
            rest = rest.replace(match_iban_bic.group(0), "").strip()
        details_opposing_account_name = rest.strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A9 — BETALING
    RE_BETALING = re.compile(
        r"^"
        r"((?:TERUG)?BETALING MET (?:DEBET\s?KAART(?: NUMMER)?|BANKKAART MET KAART)"
        r" [0-9]{4} [0-9X]{4} [0-9X]{4} [0-9X]{4}(?: [0-9X])?)"
        # group 1: details_transaction_type (part 2)
        r"( BANCONTACT PAYCONIQ CO)?"  # group 2: details_transaction_type (part 1 primary)
        r"(.*)"  # group 3: details_opposing_account_name
        r"( P2P MOBILE)?"  # group 4: details_transaction_type (part 1 primary)
        r"( [0-9]{2}/[0-9]{2}/[0-9]{4})"  # group 5: details_payment_date (date)
        r"( OM)?"  # group 6: drop
        r"( [0-9]{2}:[0-9]{2}| [0-9]{2} U [0-9]{2})?"  # group 7: details_payment_date (time)
        r"(.*?)"  # group 8: details_exchange_and_transaction_costs (optional free text)
        r"( BANCONTACT| VISA DEBIT - CONTACTLOOS| VISA DEBIT - eCommerce| VISA DEBIT)?"
        # group 9: details_transaction_type (part 1 secondary)
        r"$",
        re.IGNORECASE,
    )
    match = RE_BETALING.search(remaining_details)
    if match:
        details_match_type = "Betaling"
        details_transaction_type = (
            ((match.group(2) or "").replace(" CO", "").strip() or (match.group(9) or "").strip())
            + " "
            + (match.group(4) or "").strip()
            + " "
            + match.group(1).strip()
        )
        details_opposing_account_name = match.group(3).strip()
        time_part = ((match.group(7) or "").replace(" U ", ":") or "00:00").strip()
        details_payment_date = match.group(5).strip() + " " + time_part
        details_exchange_and_transaction_costs = match.group(8).strip() or None
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A10 — MOBIELE BETALING
    RE_MOBIELE_BETALING = re.compile(
        r"^"
        r"(MOBIELE BETALING)"  # group 1: transaction_type (part 2)
        r"( OPDRACHTGEVER REKENING : )"  # group 2: drop
        r"(.*)"  # group 3: iban, bic, name
        r"( BANCONTACT)"  # group 4: transaction_type (part 1)
        r"$",
        re.IGNORECASE,
    )
    match = RE_MOBIELE_BETALING.search(remaining_details)
    if match:
        details_match_type = "Mobiele betaling"
        details_transaction_type = match.group(4).strip() + " " + match.group(1).strip()
        rest = match.group(3).strip()
        RE_IBAN_BIC = re.compile(
            r"\b([A-Z]{2}\s*\d{2}(?:\s*[A-Z0-9]){11,30})\s+BIC\s+([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b",
            re.IGNORECASE,
        )
        match_iban_bic = RE_IBAN_BIC.search(rest)
        if match_iban_bic:
            details_opposing_account_iban = parse_iban(match_iban_bic.group(1))
            details_opposing_account_bic = match_iban_bic.group(2)
            rest = rest.replace(match_iban_bic.group(0), "").strip()
        details_opposing_account_name = rest
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A11 — GELDOPNEMING
    RE_GELDOPNEMING = re.compile(
        r"^"
        r"(GELDOPN(?:EMING|AME)(?: IN EURO)?"
        r"(?: AAN (?:ANDERE|ONZE) AUTOMATEN(?: BE)?)?"
        r" MET (?:DEBETKAART NUMMER|KAART) [0-9]{4} [0-9X]{4} [0-9X]{4} [0-9X]{4}(?: [0-9X])?)"
        # group 1: transaction_type (part 2)
        r"(.*)"  # group 2: details_opposing_account_name
        r"( [0-9]{2}/[0-9]{2}/[0-9]{4})"  # group 3: details_payment_date
        r"( [0-9]{2}:[0-9]{2}| [0-9]{2} U [0-9]{2})?"  # group 4: details_payment_date (time)
        r"( BANCONTACT| VISA DEBIT)?"  # group 5: transaction_type (part 1) — absent in older transactions
        r"$",
        re.IGNORECASE,
    )
    match = RE_GELDOPNEMING.search(remaining_details)
    if match:
        details_match_type = "Geldopneming"
        details_transaction_type = ((match.group(5) or "").strip() + " " + match.group(1).strip()).strip()
        details_opposing_account_name = match.group(2).strip()
        time_part = ((match.group(4) or "").replace(" U ", ":") or "00:00").strip()
        details_payment_date = match.group(3).strip() + " " + time_part
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # A12 - Old transactions
    if remaining_details and column_primary_transaction_date < "2018-09-01":
        details_match_type = "Old transaction"
        RE_CARDNUMBER = re.compile(r"[0-9]{4} [0-9X]{4} [0-9X]{4} [0-9X]{4}(?: [0-9X])?")
        match = RE_CARDNUMBER.search(remaining_details)
        if match:
            details_transaction_type = match.group(0).strip()
            remaining_details = remaining_details.replace(match.group(0), "").strip()
        details_opposing_account_name = remaining_details
        remaining_details = None

    # Remaining details
    if remaining_details and remaining_details not in (details_description or ""):
        if details_match_type:
            raise ValueError(
                f"Regex match ({details_match_type}), but remaining_details should be empty instead of '{remaining_details}'"  # noqa: E501
            )
        else:
            raise ValueError(f"remaining_details should be empty instead of '{remaining_details}'")

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
    if details_booking_date and details_booking_date != column_booking_date:
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

    # opposing_account_iban ← column_opposing_account_iban &| details_opposing_account_iban (optional match)
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
                normalized["opposing_account_name"] = column_opposing_account_name
        else:
            normalized["opposing_account_name"] = column_opposing_account_name
    else:
        normalized["opposing_account_name"] = details_opposing_account_name or ""

    # description ← column_description &| details_description &| column_structured_ref &| details_structured_ref
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
                if "NETTO INTERESTEN" in details_description:
                    normalized["description"] = details_description + " - " + column_description
                else:
                    raise ValueError(
                        f"Mededeling mismatch: column='{column_description}' details='{details_description}'"
                    )
        normalized["description"] = column_description
    else:
        normalized["description"] = details_description or ""

    # notes ← assembled from multiple sources
    notes = ""

    # notes — details_bank_reference
    if details_bank_reference:
        notes = append_note_line(notes, "details_bank_reference", details_bank_reference)

    # notes — details_technical_reference
    if details_technical_reference:
        notes = append_note_line(notes, "details_technical_reference", details_technical_reference)

    # notes — details_exchange_and_transaction_costs
    if details_exchange_and_transaction_costs:
        notes = append_note_line(
            notes, "details_exchange_and_transaction_costs", details_exchange_and_transaction_costs
        )

    column_transaction_type_norm = normalize_for_comparison(column_transaction_type).removesuffix("INEURO")
    details_transaction_type_norm = normalize_for_comparison(details_transaction_type or "")

    # notes — details_transaction_type
    if (
        details_transaction_type
        and column_transaction_type_norm == "INSTANTOVERSCHRIJVING"
        and details_transaction_type_norm.startswith("WEROOVERSCHRIJVING")
    ):
        details_transaction_type = details_transaction_type.replace("OVERSCHRIJVING", "INSTANTOVERSCHRIJVING")
    if (
        details_transaction_type
        and column_transaction_type_norm == "CORRECTIEKAARTVERRICHTING"
        and details_transaction_type_norm.startswith("STORTINGOPDEREKENINGGEKOPPELDAANDEDEBETKAART")
    ):
        details_transaction_type = details_transaction_type.replace("STORTING", "(CORRECTIE) STORTING")
    if (
        details_transaction_type
        and column_transaction_type_norm == "KAARTBETALING"
        and details_transaction_type_norm == "BANCONTACTMOBIELEBETALING"
    ):
        details_transaction_type = details_transaction_type.replace("BETALING", "KAARTBETALING")
    if (
        column_transaction_type_norm == "DOORLOPENDEBETALINGSOPDRACHT"
        and details_transaction_type_norm == "DOORLOPENDEOPDRACHT"
    ) or details_transaction_type_norm in column_transaction_type_norm:
        details_transaction_type = None
    if details_transaction_type:
        notes = append_note_line(notes, "details_transaction_type", details_transaction_type)

    # notes — column_transaction_type
    if not column_transaction_type:
        raise ValueError("Missing transaction type")
    if (
        (
            "KAARTBETALING" in column_transaction_type_norm
            and (
                "BETALINGMETDEBETKAART" in details_transaction_type_norm
                or "BETALINGMETBANKKAART" in details_transaction_type_norm
            )
        )
        or column_transaction_type_norm in details_transaction_type_norm
        or (
            column_transaction_type_norm == "INSTANTOVERSCHRIJVING"
            and details_transaction_type_norm.startswith("WEROOVERSCHRIJVING")
        )
        or (
            column_transaction_type_norm == "CORRECTIEKAARTVERRICHTING"
            and details_transaction_type_norm.startswith("STORTINGOPDEREKENINGGEKOPPELDAANDEDEBETKAART")
        )
        or (
            column_transaction_type_norm == "KAARTBETALING"
            and details_transaction_type_norm == "BANCONTACTMOBIELEBETALING"
        )
        or (
            column_transaction_type_norm == "GELDOPNAMEMETKAART"
            and details_transaction_type_norm == "GELDOPNAMEAANANDEREAUTOMATENMETKAART"
        )
        or (
            column_transaction_type_norm == "INSTANTOVERSCHRIJVING"
            and details_transaction_type_norm == "INSTANTEUROPESEOVERSCHRIJVING"
        )
    ):
        column_transaction_type = None

    if column_transaction_type:
        notes = append_note_line(notes, "column_transaction_type", column_transaction_type)

    # Write the notes column
    normalized["notes"] = notes

    return normalized
