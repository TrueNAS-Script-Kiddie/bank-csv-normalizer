"""
Import normalized CSVs from data/normalized/ into Firefly III via its REST API.

One POST per transaction. Per file:
  - every row imported / already present -> file moved to data/imported/
  - some rows failed -> failed rows to data/failed/<ts>-<name>-import-failed.csv,
    file moved to data/imported/ as -imported-partial, alert on stderr
  - Firefly unreachable or token refused -> run stops, file stays for the next run;
    alerted once per outage (flag file), not every cron minute

Re-running a file is safe: error_if_duplicate_hash makes Firefly reject a
transaction it already stored, which counts as "already present".

Transfers between own asset accounts appear in both accounts' CSVs but are one
transaction in Firefly ("match or create"): a row whose counterparty is an own
asset account first looks for an existing transfer between the same two
accounts, same amount, dated within TRANSFER_MATCH_DAYS. Each transfer can be
claimed once per side (outgoing / incoming). Found -> already present;
not found -> the row creates the transfer. Order and history coverage of the
accounts' CSVs therefore don't matter.

Usage:
  PYTHONPATH=. python3 -m engine.firefly.import_normalized [--dry-run] [--show N] [csv ...]
"""

import argparse
import collections
import csv
import glob
import json
import os
import re
import shutil
import sys
from datetime import date
from decimal import Decimal
from typing import Any

from engine.core.runtime import BASE_DIR, CONFIG, alert, log_event
from engine.firefly.api import FireflyAuthError, FireflyClient, FireflyUnavailableError

DATA_DIR = os.path.join(BASE_DIR, "data")
NORMALIZED_DIR = os.path.join(DATA_DIR, "normalized")
IMPORTED_DIR = os.path.join(DATA_DIR, "imported")
FAILED_DIR = os.path.join(DATA_DIR, "failed")
LOG_DIR = os.path.join(DATA_DIR, "logs")
BLOCKED_FLAG = os.path.join(DATA_DIR, "firefly-import-blocked.flag")

EXIT_OK = 0
EXIT_UNAVAILABLE = 69
EXIT_PARTIAL = 75

TRANSFER_MATCH_DAYS = 7
UNKNOWN_COUNTERPARTY = "(onbekend)"

RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RE_DATE_OPTIONAL_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2})?$")
RE_AMOUNT = re.compile(r"^-?\d+(\.\d+)?$")
RE_DUPLICATE = re.compile(r"Duplicate of transaction #(\d+)")

# (source_id, destination_id, amount) -> transfers: {"date": date, "out": claimed, "in": claimed}
TransferPool = dict[tuple[str, str, str], list[dict[str, Any]]]


def clean_iban(value: str | None) -> str:
    return (value or "").replace(" ", "").upper()


def money(value: str) -> str:
    """Canonical amount for matching; Firefly returns amounts with many decimals."""
    return str(Decimal(value).copy_abs().quantize(Decimal("0.01")))


# ---------------------------------------------------------------------------
# Firefly state
# ---------------------------------------------------------------------------
def load_asset_accounts(client: FireflyClient) -> dict[str, dict[str, str]]:
    """IBAN -> {"id", "name"} for every Firefly asset account that has an IBAN."""
    assets: dict[str, dict[str, str]] = {}
    for account in client.get_all("accounts?type=asset"):
        iban = clean_iban(account["attributes"].get("iban"))
        if iban:
            assets[iban] = {"id": str(account["id"]), "name": account["attributes"]["name"]}
    return assets


def load_transfer_pool(client: FireflyClient) -> TransferPool:
    pool: TransferPool = collections.defaultdict(list)
    for group in client.get_all("transactions?type=transfer"):
        for split in group["attributes"]["transactions"]:
            key = (str(split["source_id"]), str(split["destination_id"]), money(split["amount"]))
            pool[key].append({"date": date.fromisoformat(split["date"][:10]), "out": False, "in": False})
    return pool


def claim_transfer(pool: TransferPool, key: tuple[str, str, str], on: date, side: str) -> bool:
    """Claim the closest unclaimed transfer for this side within the match window."""
    candidates = [t for t in pool.get(key, []) if not t[side] and abs((t["date"] - on).days) <= TRANSFER_MATCH_DAYS]
    if not candidates:
        return False
    min(candidates, key=lambda t: abs((t["date"] - on).days))[side] = True
    return True


# ---------------------------------------------------------------------------
# Row -> Firefly split
# ---------------------------------------------------------------------------
def build_split(
    row: dict[str, str],
    assets: dict[str, dict[str, str]],
) -> tuple[str, dict[str, Any], tuple[tuple[str, str, str], str] | None]:
    """
    Return (decision, split, transfer_match). decision: withdrawal / deposit / transfer.
    transfer_match is ((source_id, destination_id, amount), side) for own transfers. Raises ValueError.
    """
    amount = row["amount"].strip()
    if not RE_AMOUNT.match(amount) or Decimal(amount) == 0:
        raise ValueError(f"Invalid or zero amount: '{amount}'")

    for field, pattern in (
        ("primary_transaction_date", RE_DATE),
        ("booking_date", RE_DATE),
        ("transaction_processing_date", RE_DATE),
        ("payment_date", RE_DATE_OPTIONAL_TIME),
    ):
        value = row.get(field, "")
        if value and not pattern.match(value):
            raise ValueError(f"{field} not ISO: '{value}'")
    if not row.get("primary_transaction_date"):
        raise ValueError("primary_transaction_date is empty")

    own_iban = clean_iban(row["asset_account_iban"])
    own = assets.get(own_iban)
    if not own:
        raise ValueError(f"No Firefly asset account with IBAN {own_iban}")

    opposing_iban = clean_iban(row.get("opposing_account_iban"))
    opposing_asset = assets.get(opposing_iban) if opposing_iban and opposing_iban != own_iban else None
    outgoing = amount.startswith("-")

    # Without a counterparty Firefly books on its Cash account: right for cash withdrawals only
    opposing_name = row.get("opposing_account_name", "")
    if not opposing_name and not opposing_iban:
        is_cash_withdrawal = outgoing and "geldopn" in row.get("unmapped_transaction_type", "").lower()
        opposing_name = "" if is_cash_withdrawal else UNKNOWN_COUNTERPARTY

    # Firefly requires a description; fall back to what identifies the transaction best
    notes = row.get("notes", "")
    description = row.get("description", "") or opposing_name or notes.split("\n", 1)[0]

    split: dict[str, Any] = {
        "date": row["primary_transaction_date"],
        "amount": amount.lstrip("-"),
        "currency_code": row.get("account_currency_code", ""),
        "description": description,
        "notes": notes,
        "external_id": row.get("external_id", ""),
        "book_date": row.get("booking_date", ""),
        "process_date": row.get("transaction_processing_date", ""),
        "payment_date": row.get("payment_date", ""),
    }

    transfer_match = None
    if opposing_asset:
        source, destination = (own, opposing_asset) if outgoing else (opposing_asset, own)
        split.update(type="transfer", source_id=source["id"], destination_id=destination["id"])
        transfer_match = ((source["id"], destination["id"], money(amount)), "out" if outgoing else "in")
        decision = "transfer"
    elif outgoing:
        split.update(
            type="withdrawal",
            source_id=own["id"],
            destination_name=opposing_name,
            destination_iban=opposing_iban,
            destination_bic=row.get("opposing_account_bic", ""),
        )
        decision = "withdrawal"
    else:
        split.update(
            type="deposit",
            destination_id=own["id"],
            source_name=opposing_name,
            source_iban=opposing_iban,
            source_bic=row.get("opposing_account_bic", ""),
        )
        decision = "deposit"

    # Firefly treats an absent key as "not set"; empty strings can trip its validators
    return decision, {key: value for key, value in split.items() if value != ""}, transfer_match


# ---------------------------------------------------------------------------
# One file
# ---------------------------------------------------------------------------
def import_file(
    path: str,
    client: FireflyClient,
    assets: dict[str, dict[str, str]],
    pool: TransferPool,
    dry_run: bool,
    show: int,
) -> collections.Counter:
    name = os.path.basename(path)
    # '<ts>-<name>-normalized[-partial].csv' -> '<ts>-<name>': every output keeps the
    # normalizer's run timestamp, so all files of one bank CSV sort together
    base = re.sub(r"-normalized(-partial)?$", "", os.path.splitext(name)[0])
    logfile = os.path.join(LOG_DIR, f"{base}-import.log")

    def log(message: str) -> None:
        if dry_run:
            print(f"  {message}")
        else:
            log_event(logfile, message)

    counts: collections.Counter = collections.Counter()
    failed: list[tuple[dict[str, str], str]] = []
    shown = 0

    with open(path, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter=";"))
    log(f"Importing {name}: {len(rows)} rows")

    for line_no, row in enumerate(rows, start=2):
        try:
            decision, split, transfer_match = build_split(row, assets)
        except (ValueError, KeyError, ArithmeticError) as exc:
            failed.append((row, f"line {line_no}: {exc}"))
            counts["failed"] += 1
            continue

        if transfer_match:
            key, side = transfer_match
            on = date.fromisoformat(split["date"])
            if claim_transfer(pool, key, on, side):
                counts["transfer already present"] += 1
                continue

        if dry_run:
            status, response = 200, {}
            if shown < show:
                print(json.dumps(split, ensure_ascii=False))
                shown += 1
        else:
            body = {
                "error_if_duplicate_hash": True,
                "apply_rules": True,
                "fire_webhooks": True,
                "transactions": [split],
            }
            status, response = client.request("POST", "transactions", body)

        if status == 200:
            counts[decision] += 1
        elif status == 422 and RE_DUPLICATE.search(json.dumps(response)):
            counts["already present"] += 1
        else:
            reason = f"line {line_no}: HTTP {status}: {response.get('message', '')} {response.get('errors', '')}"
            failed.append((row, reason))
            counts["failed"] += 1
            log(reason)
            continue

        if transfer_match:
            # The other side of this transfer must find it later, in this run or a future one
            key, side = transfer_match
            pool.setdefault(key, []).append(
                {"date": date.fromisoformat(split["date"]), "out": side == "out", "in": side == "in"}
            )

    log(f"Result {name}: {dict(counts)}")
    for _, reason in failed:
        log(f"FAILED {reason}")

    if dry_run:
        return counts

    target_name = f"{base}-imported.csv"
    if failed:
        failed_path = os.path.join(FAILED_DIR, f"{base}-import-failed.csv")
        with open(failed_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[*rows[0].keys(), "import_error"], delimiter=";")
            writer.writeheader()
            for row, reason in failed:
                writer.writerow({**row, "import_error": reason})
        target_name = f"{base}-imported-partial.csv"
        reasons = "\n".join(reason for _, reason in failed[:20])
        alert(
            f"FIREFLY IMPORT PARTIAL: {name} ({counts['failed']} of {len(rows)} rows failed)",
            f"File: {name}\nFailed rows: {failed_path}\nLog: {logfile}\n{dict(counts)}\n\n{reasons}",
        )
    shutil.move(path, os.path.join(IMPORTED_DIR, target_name))
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def set_blocked(reason: str) -> None:
    """Alert once per outage; the cron runs every minute."""
    if not os.path.exists(BLOCKED_FLAG):
        alert(f"FIREFLY IMPORT BLOCKED: {reason}", f"{reason}\n\nFiles stay in {NORMALIZED_DIR} and are retried.")
    with open(BLOCKED_FLAG, "w", encoding="utf-8") as f:
        f.write(reason + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="build every request, send nothing, move nothing")
    parser.add_argument("--show", type=int, default=0, help="dry-run: print the first N request payloads per file")
    parser.add_argument("files", nargs="*", help=f"default: every *.csv in {NORMALIZED_DIR}")
    args = parser.parse_args()

    files = args.files or sorted(glob.glob(os.path.join(NORMALIZED_DIR, "*.csv")))
    if not files:
        return EXIT_OK

    if not args.dry_run:
        for d in (IMPORTED_DIR, FAILED_DIR, LOG_DIR):
            os.makedirs(d, exist_ok=True)

    totals: collections.Counter = collections.Counter()
    try:
        client = FireflyClient(CONFIG.get("FIREFLY_URL", ""), CONFIG.get("FIREFLY_TOKEN", ""))
        assets = load_asset_accounts(client)
        pool = load_transfer_pool(client)
        for path in files:
            if args.dry_run:
                print(f"== {os.path.basename(path)}")
            counts = import_file(path, client, assets, pool, args.dry_run, args.show)
            if args.dry_run:
                print(f"  {dict(counts)}")
            totals.update(counts)
    except (FireflyAuthError, FireflyUnavailableError) as exc:
        if args.dry_run:
            print(f"BLOCKED: {exc}", file=sys.stderr)
        else:
            set_blocked(str(exc))
        return EXIT_UNAVAILABLE

    if not args.dry_run and os.path.exists(BLOCKED_FLAG):
        os.remove(BLOCKED_FLAG)
    if args.dry_run:
        print(f"== TOTAL {dict(totals)}")
    return EXIT_PARTIAL if totals["failed"] else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
