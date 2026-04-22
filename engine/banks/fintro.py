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
    column_transaction_type = csv_row["transaction_type"].replace(" in euro", "")
    column_primary_transaction_date = parse_ddmmyyyy(csv_row["primary_transaction_date"])
    column_booking_date = parse_ddmmyyyy(csv_row["booking_date"])

    # -- Multi purpose column: details --
    remaining_details = csv_row.get("details", "")

    details_booking_date = ""
    details_bank_reference = ""
    details_transaction_processing_date = ""
    details_description = ""
    details_opposing_account_name = ""
    details_transaction_type = ""
    details_payment_date = ""
    details_opposing_account_iban = ""
    details_opposing_account_bic = ""
    details_technical_reference = ""
    details_exchange_and_transaction_costs = ""

    details_match_type = ""

    # VALUTADATUM -> details_booking_date
    RE_BOOKING_DATE = re.compile(r"VALUTADATUM\s*:\s*(\d{2}/\d{2}/\d{4})$")
    match = RE_BOOKING_DATE.search(remaining_details)
    if match:
        details_booking_date = parse_ddmmyyyy(match.group(1))
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # BANKREFERENTIE -> details_bank_reference
    RE_BANK_REFERENCE = re.compile(r"BANKREFERENTIE\s*:\s*([0-9]+)$")
    match = RE_BANK_REFERENCE.search(remaining_details)
    if match:
        details_bank_reference = match.group(1)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # UITGEVOERD OP -> details_transaction_processing_date
    RE_TRANSACTION_PROCESSING_DATE = re.compile(r"UITGEVOERD OP\s+(\d{2}/\d{2}(?:/\d{4})?)$")
    match = RE_TRANSACTION_PROCESSING_DATE.search(remaining_details)
    if match:
        details_transaction_processing_date = parse_ddmmyyyy(
            match.group(1), fallback_date_str=column_primary_transaction_date
        )
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # MEDEDELING, TERUGBETALING WOONKREDIET, MAANDELIJKSE BIJDRAGE, BONUS, NETTO INTERESTEN, EQUIPERINGSKOSTEN,
    # GEBRUIKSKOSTEN, EFFECTEN, KREDIET -> details_description
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

    # ZONDER MEDEDELING -> details_no_description
    details_no_description = False
    RE_NO_DESCRIPTION = re.compile(r"\bZONDER\s+MEDEDELING\b$")
    match = RE_NO_DESCRIPTION.search(remaining_details)
    if match:
        details_match_type = "No description"
        details_no_description = True
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # STORTING -> details_opposing_account_name, details_payment_date, details_transaction_type
    RE_STORTING = re.compile(
        r"^(STORTING)"  # group 1: details_transaction_type
        r"( VAN )"  # group 2: drop
        r"(.+?)"  # group 3: details_opposing_account_name
        r"( OP DE REKENING GEKOPPELD AAN DE DEBETKAART NUMMER )"  # group 4: details_transaction_type
        r"([0-9]{4}\s[0-9]{2}XX\sXXXX\s(?:[0-9]{4}|X[0-9]{3}\s[0-9]))"  # group 5: details_transaction_type
        r"( [0-9]{2}/[0-9]{2}/[0-9]{4})"  # group 6: details_payment_date
    )
    match = RE_STORTING.search(remaining_details)
    if match:
        details_match_type = "Storting"
        details_opposing_account_name = match.group(3).strip()
        details_payment_date = match.group(6).strip()
        details_transaction_type = match.group(1) + match.group(4) + match.group(5)
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # DOORLOPENDE OPDRACHT -> details_transaction_type, details_opposing_account_name
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

    # DOMICILIERING -> details_transaction_type, details_opposing_account_name, details_dom_date,
    # details_technical_reference
    RE_DOMICILIERING = re.compile(
        r"^(?:(EERSTE INVORDERING VAN EEN ))?"  # group 1: details_transaction_type optional prefix
        r"((?:GEWEIGERDE )?EUROPESE DOMICILIERING)"  # group 2: details_transaction_type core
        r"( VAN )"  # group 3: drop
        r"(.+?)"  # group 4: details_opposing_account_name
        r"(?: DATUM : ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}))?"  # group 5: details_dom_date optional
        r"( MANDAAT NUMMER : )"  # group 6: details_technical_reference fixed
        r"([A-Z0-9\-]+)"  # group 7: details_technical_reference mandate number
        r"( REFERTE : )"  # group 8: details_technical_reference fixed
        r"(.*)$"  # group 9: details_technical_reference reference
    )
    match = RE_DOMICILIERING.search(remaining_details)
    if match:
        details_match_type = "Domiciliering"
        details_transaction_type = ((match.group(1) or "") + match.group(2)).strip()
        details_opposing_account_name = match.group(4).strip()
        details_dom_date = (match.group(5) or "").strip()
        details_technical_reference = (match.group(6) + match.group(7) + match.group(8) + match.group(9)).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # OVERSCHRIJVING -> details_transaction_type, details_technical_reference, details_opposing_account_iban,
    # details_opposing_account_bic, details_opposing_account_name
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

    # BETALING -> details_transaction_type, details_opposing_account_name, details_payment_date,
    # details_exchange_and_transaction_costs
    RE_BETALING = re.compile(
        r"^"
        r"((?:TERUG)?BETALING MET (?:DEBET\s?KAART(?: NUMMER)?|BANKKAART MET KAART)"
        r" [0-9]{4}\s[0-9]{2}XX\sXXXX\s(?:[0-9]{4}|X[0-9]{3}\s[0-9]))"
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
        ).strip()
        details_opposing_account_name = match.group(3).strip()
        time_part = ((match.group(7) or "").replace(" U ", ":") or "00:00").strip()
        details_payment_date = match.group(5).strip() + " " + time_part
        details_exchange_and_transaction_costs = match.group(8).strip()
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # MOBIELE BETALING -> details_transaction_type, details_opposing_account_iban, details_opposing_account_bic,
    # details_opposing_account_name
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

    # GELDOPNEMING -> details_transaction_type, details_opposing_account_name, details_payment_date
    RE_GELDOPNEMING = re.compile(
        r"^"
        r"(GELDOPN(?:EMING|AME)(?: IN EURO)?"
        r"(?: AAN (?:ANDERE|ONZE) AUTOMATEN(?: BE)?)?"
        r" MET (?:DEBETKAART NUMMER|KAART) [0-9]{4}\s[0-9]{2}XX\sXXXX\s(?:[0-9]{4}|X[0-9]{3}\s[0-9]))"
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
        details_transaction_type = (
            (match.group(5) or "").strip() + " " + match.group(1).strip().replace(" IN EURO", "")
        ).strip()
        details_opposing_account_name = match.group(2).strip()
        time_part = ((match.group(4) or "").replace(" U ", ":") or "00:00").strip()
        details_payment_date = match.group(3).strip() + " " + time_part
        remaining_details = remaining_details.replace(match.group(0), "").strip()

    # Old transactions -> details_transaction_type, details_opposing_account_name
    if remaining_details and column_primary_transaction_date < "2018-09-01":
        RE_CARDNUMBER = re.compile(
            r"^"
            r"([0-9]{4}\s[0-9]{2}XX\sXXXX\s[0-9]{4})?"  # group 1: details_transaction_type
            r"(?:\s([0-9]))?"  # group 2: drop
            r"(.*)"  # group 3: details_opposing_account_name
            r"$"
        )
        match = RE_CARDNUMBER.search(remaining_details)
        if match:
            details_match_type = "Old transaction"
            if match.group(1):
                details_transaction_type = match.group(1).strip()
            details_opposing_account_name = match.group(3).strip()
            remaining_details = ""

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

    # column_external_id -> external_id
    normalized["external_id"] = column_external_id

    # column_primary_transaction_date -> primary_transaction_date
    normalized["primary_transaction_date"] = column_primary_transaction_date

    # details_transaction_processing_date -> transaction_processing_date
    normalized["transaction_processing_date"] = details_transaction_processing_date

    # column_booking_date &| details_booking_date (match) -> booking_date
    if details_booking_date and details_booking_date != column_booking_date:
        raise ValueError(
            "Value date mismatch between dedicated column and details: "
            f"column_booking_date='{column_booking_date}' details_booking_date='{details_booking_date}'"
        )
    normalized["booking_date"] = column_booking_date

    # details_payment_date -> payment_date
    normalized["payment_date"] = details_payment_date

    # column_amount -> amount
    normalized["amount"] = column_amount.replace(",", ".").strip()

    # column_account_currency_code -> account_currency_code
    if column_account_currency_code != "EUR":
        raise ValueError("Non-EUR account currency")
    normalized["account_currency_code"] = column_account_currency_code

    # column_asset_account_iban -> asset_account_iban
    normalized["asset_account_iban"] = column_asset_account_iban.replace(" ", "").upper()

    # column_opposing_account_iban &| details_opposing_account_iban (optional match) -> opposing_account_iban
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
    normalized["opposing_account_iban"] = column_opposing_account_iban or details_opposing_account_iban

    # details_opposing_account_bic -> opposing_account_bic
    normalized["opposing_account_bic"] = details_opposing_account_bic

    # column_opposing_account_name &| details_opposing_account_name (append) -> opposing_account_name
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
        normalized["opposing_account_name"] = details_opposing_account_name

    # column_description &| details_description &| column_structured_ref &| details_structured_ref -> description
    if details_no_description and (column_description or details_description):
        raise ValueError("ZONDER MEDEDELING present but description found in a dedicated column or details column")
    REPLACE_IN_DETAILS_DESCRIPTION = [
        ("NETTO INTERESTEN", "Netto interesten : "),
        ("MAANDELIJKSE BIJDRAGE FINTRO BLUE", "Maandelijkse bijdrage voor Fintro Blue"),
        ("TERUGBETALING WOONKREDIET", "Terugbetaling woonkrediet"),
        ("UITBETALING VAN UW BONUS", "Uitbetaling van uw bonus"),
        ("MAANDELIJKSE EQUIPERINGSKOSTEN VOOR DE PERIODE", "Maandelijkse equiperingskosten voor de periode"),
        (" TOT ", " tot "),
        ("GEBRUIKSKOSTEN VOOR DE PERIODE", "Gebruikskosten voor de periode"),
        (
            "INSCHRIJVING OP BELGISCHE EFFECTEN REFERTE",
            "Inschrijving op de Belgische effecten (pensioensparen), referte",
        ),
        ("INTEREST KREDIET", "Interest krediet"),
        ("DOSSIERKOSTEN VOOR KREDIET", "Dossierkosten voor krediet"),
        ("VERVROEGDE TERUGBETALING KREDIET", "Vervroegde terugbetaling krediet"),
    ]
    for old_value, new_value in REPLACE_IN_DETAILS_DESCRIPTION:
        details_description = details_description.replace(old_value, new_value)
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
                if "Netto interesten : " in details_description:
                    normalized["description"] = details_description + column_description
                else:
                    raise ValueError(
                        f"Mededeling mismatch: column='{column_description}' details='{details_description}'"
                    )
            else:
                normalized["description"] = column_description
        else:
            normalized["description"] = column_description
    else:
        normalized["description"] = details_description
        if (
            details_description.startswith("Uitbetaling van uw bonus")
            and column_transaction_type == "Kosten rekeningbeheer"
        ):
            column_transaction_type = "Opbrengsten in verband met de rekening"

    # mix -> notes
    notes = ""
    references: str = ""

    # details_exchange_and_transaction_costs -> notes
    if details_exchange_and_transaction_costs:
        REPLACE_IN_DETAILS_EXCHANGE_AND_TRANSACTION_COSTS = [
            ("BEHANDELINGSKOSTEN", "Behandelingskosten"),
            ("KOERS", "Koers:"),
            ("WISSELKOSTEN", "Wisselkosten"),
        ]
        for old_value, new_value in REPLACE_IN_DETAILS_EXCHANGE_AND_TRANSACTION_COSTS:
            details_exchange_and_transaction_costs = details_exchange_and_transaction_costs.replace(
                old_value, new_value
            )
        details_eatc_parts = details_exchange_and_transaction_costs.split()
        if len(details_eatc_parts) != 2 or not (
            details_eatc_parts[0] == column_account_currency_code
            and float(details_eatc_parts[1].replace(",", ".")) == abs(float(column_amount.replace(",", ".")))
        ):
            if re.match(r"^[A-Za-z]{3} \d", details_exchange_and_transaction_costs):
                sign = "-" if column_amount.startswith("-") else ""
                details_exchange_and_transaction_costs = (
                    "Bedrag: " + details_eatc_parts[0] + " " + sign + " ".join(details_eatc_parts[1:])
                )
            notes = (
                f"{notes}\n{details_exchange_and_transaction_costs}"
                if notes
                else details_exchange_and_transaction_costs
            )

    # Prepare details_transaction_type and column_transaction_type
    if column_transaction_type:
        column_transaction_type_norm = normalize_for_comparison(column_transaction_type).removesuffix("INEURO")
    else:
        raise ValueError("Missing transaction type")

    if details_transaction_type:
        details_transaction_type_norm = normalize_for_comparison(details_transaction_type)
        if column_transaction_type_norm == "INSTANTOVERSCHRIJVING" and details_transaction_type_norm.startswith(
            "WEROOVERSCHRIJVING"
        ):
            details_transaction_type = details_transaction_type.replace("OVERSCHRIJVING", "INSTANTOVERSCHRIJVING")
            column_transaction_type = ""
        elif column_transaction_type_norm == "CORRECTIEKAARTVERRICHTING" and (
            details_transaction_type_norm.startswith("STORTINGOPDEREKENINGGEKOPPELDAANDEDEBETKAART")
            or "TERUGBETALINGMETDEBETKAART" in details_transaction_type_norm
        ):
            details_transaction_type = "(Correctie) " + details_transaction_type
            column_transaction_type = ""
        elif (
            column_transaction_type_norm == "KAARTBETALING"
            and details_transaction_type_norm == "BANCONTACTMOBIELEBETALING"
        ):
            details_transaction_type = details_transaction_type.replace("BETALING", "KAARTBETALING")
            column_transaction_type = ""
        elif "KAARTBETALING" in column_transaction_type_norm and (
            "BETALINGMETDEBETKAART" in details_transaction_type_norm
            or "BETALINGMETBANKKAART" in details_transaction_type_norm
        ):
            column_transaction_type = ""
        elif "GELDOPNAME" in column_transaction_type_norm and (
            "GELDOPNAME" in details_transaction_type_norm or "GELDOPNEMING" in details_transaction_type_norm
        ):
            column_transaction_type = ""
        elif (
            column_transaction_type_norm == "INSTANTOVERSCHRIJVING"
            and details_transaction_type_norm == "INSTANTEUROPESEOVERSCHRIJVING"
        ):
            column_transaction_type = ""
        elif (
            column_transaction_type_norm == "DOORLOPENDEBETALINGSOPDRACHT"
            and details_transaction_type_norm == "DOORLOPENDEOPDRACHT"
        ):
            details_transaction_type = ""
        elif (
            column_transaction_type_norm == "TEGENBOEKINGBETAALDEDOMICILIERING"
            and details_transaction_type_norm == "GEWEIGERDEEUROPESEDOMICILIERING"
        ):
            column_transaction_type = "Tegenboeking van geweigerde / betaalde / Europese Domiciliëring" + (
                f" op datum {details_dom_date}" if details_dom_date else ""
            )
            details_transaction_type = ""
        elif column_transaction_type_norm == "AFLOSSINGKREDIET" and details_transaction_type_norm == "OVERSCHRIJVING":
            details_transaction_type = "Overschrijving voor aflossing krediet"
            column_transaction_type = ""
        elif details_transaction_type.startswith("6703 04XX XXXX"):
            if column_transaction_type == "Kaartbetaling":
                details_transaction_type = "Betaling met debetkaart " + details_transaction_type
                column_transaction_type = ""
            elif column_transaction_type == "Geldopname met kaart":
                details_transaction_type = "Geldopneming met debetkaart " + details_transaction_type
                column_transaction_type = ""
            elif column_transaction_type == "Geldopname in buitenland":
                details_transaction_type = "Geldopneming in buitenland met debetkaart " + details_transaction_type
                column_transaction_type = ""
        elif details_transaction_type_norm in column_transaction_type_norm:
            details_transaction_type = ""
        elif column_transaction_type_norm in details_transaction_type_norm:
            column_transaction_type = ""

    if column_transaction_type == "Effecteninschrijving" and "Inschrijving op de Belgische effecten" in (
        details_description
    ):
        column_transaction_type = ""

    if column_transaction_type and details_transaction_type:
        raise ValueError(
            f"Both 'column_transaction_type' ({column_transaction_type}) and "
            f"'details_transaction_type' ({details_transaction_type}) have a value.\n"
            "By now, at least one of them should be empty. A normalisation seems missing..."
        )

    # details_transaction_type -> notes
    if details_transaction_type:
        REPLACE_IN_DETAILS_TRANSACTION_TYPE = [
            ("  ", " "),
            ("BETALING MET BANKKAART MET KAART", "Betaling met debetkaart"),
            ("BETALING MET DEBET KAART NUMMER", "Betaling met debetkaart"),
            ("BETALING MET DEBETKAART NUMMER", "Betaling met debetkaart"),
            ("BANCONTACT Betaling", "Bancontact betaling"),
            ("GELDOPNEMING MET DEBETKAART NUMMER", "Geldopneming met debetkaart"),
            ("GELDOPNAME AAN ANDERE AUTOMATEN MET KAART", "Geldopneming aan andere automaten met debetkaart"),
            ("GELDOPNAME AAN ONZE AUTOMATEN MET KAART", "Geldopneming aan onze automaten met debetkaart"),
            ("GELDOPNEMING AAN ANDERE AUTOMATEN MET KAART", "Geldopneming aan andere automaten met debetkaart"),
            ("GELDOPNEMING AAN ONZE AUTOMATEN MET KAART", "Geldopneming aan onze automaten met debetkaart"),
            (
                "GELDOPNEMING AAN ANDERE AUTOMATEN MET DEBETKAART NUMMER",
                "Geldopneming aan andere automaten met debetkaart",
            ),
            ("GELDOPNEMING AAN ONZE AUTOMATEN MET DEBETKAART NUMMER", "Geldopneming aan onze automaten met debetkaart"),
            (
                "GELDOPNEMING AAN ONZE AUTOMATEN BE MET DEBETKAART NUMMER",
                "Geldopneming aan onze automaten met debetkaart",
            ),
            ("INSTANT EUROPESE OVERSCHRIJVING", "Europese instantoverschrijving"),
            ("INSTANT OVERSCHRIJVING", "Instantoverschrijving"),
            ("INSTANTOVERSCHRIJVING", "Instantoverschrijving"),
            ("EERSTE INVORDERING VAN EEN EUROPESE DOMICILIERING", "Eerste invordering van een Europese domiciliëring"),
            ("EUROPESE DOMICILIERING", "Europese domiciliëring"),
            ("EUROPESE OVERSCHRIJVING", "Europese overschrijving"),
            ("VIA WEB BANKING", "via Web Banking"),
            ("VIA MOBILE BANKING", "via Mobile Banking"),
            ("OVERSCHRIJVING", "Overschrijving"),
            ("TERUGBETALING MET DEBETKAART", "Terugbetaling met debetkaart"),
            ("BANCONTACT Betaling", "Bancontact betaling"),
            ("BANCONTACT Terugbetaling", "Bancontact terugbetaling"),
            ("BANCONTACT Geldopneming", "Bancontact geldopneming"),
            ("BANCONTACT PAYCONIQ Betaling", "Bancontact Payconiq betaling"),
            ("WERO Instantoverschrijving", "Wero instantoverschrijving"),
            ("VISA DEBIT - CONTACTLOOS Betaling", "Visa Debit (contactloos) betaling"),
            ("VISA DEBIT - eCommerce Betaling", "Visa Debit (eCommerce) betaling"),
            ("VISA DEBIT Betaling", "Visa Debit betaling"),
            ("VISA DEBIT Geldopneming", "Visa Debit geldopneming"),
            ("BANCONTACT MOBIELE KAARTBETALING", "Mobiele betaling met debetkaart"),
            (
                "STORTING OP DE REKENING GEKOPPELD AAN DE DEBETKAART NUMMER",
                "Storting op de rekening gekoppeld aan de debetkaart",
            ),
        ]
        for old_value, new_value in REPLACE_IN_DETAILS_TRANSACTION_TYPE:
            details_transaction_type = details_transaction_type.replace(old_value, new_value)
        RE_NORMALIZE_CARDNUMBER = re.compile(r"\b(\d{4} \d{2}XX XXXX) X(\d{3}) (\d)\b")
        details_transaction_type = RE_NORMALIZE_CARDNUMBER.sub(
            lambda match: f"{match.group(1)} {match.group(2)}{match.group(3)}", details_transaction_type
        )
        notes = f"{notes}\n{details_transaction_type}" if notes else details_transaction_type

    # column_transaction_type -> notes
    if column_transaction_type:
        REPLACE_IN_COLUMN_TRANSACTION_TYPE = [
            ("Diverse Debetverrichtingen", "Diverse debetverrichtingen"),
            ("Hypotheekleningen Terugbetalingen", "Hypotheekleningen terugbetalingen"),
            ("Kaartbetaling", "Betaling met debetkaart"),
            ("Geldopname met kaart", "Geldopneming met debetkaart"),
        ]
        for old_value, new_value in REPLACE_IN_COLUMN_TRANSACTION_TYPE:
            column_transaction_type = column_transaction_type.replace(old_value, new_value)
        notes = f"{notes}\n{column_transaction_type}" if notes else column_transaction_type

    # details_bank_reference -> notes
    if details_bank_reference:
        references = f"Bankreferentie: {details_bank_reference}"

    # details_technical_reference -> notes
    if details_technical_reference:
        REPLACE_IN_DETAILS_TECHNICAL_REFERENCE = [
            ("MANDAAT NUMMER :", "Mandaat nummer:"),
            ("REFERTE OPDRACHTGEVER :", "Referte opdrachtgever:"),
            ("UW REFERTE :", "Uw referte:"),
            ("REFERTE :", "Referte:"),
        ]
        for old_value, new_value in REPLACE_IN_DETAILS_TECHNICAL_REFERENCE:
            details_technical_reference = details_technical_reference.replace(old_value, new_value)
        if references:
            references += " "
        references += details_technical_reference

    if references:
        notes = f"{notes}\n{references}" if notes else references

    # Write the notes column
    normalized["notes"] = notes

    return normalized
