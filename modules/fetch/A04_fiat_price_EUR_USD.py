# This script reads transaction timestamps from database/data_decode_txs.csv,
# fetches USD/EUR and EUR/USD rates for each timestamp’s UTC day,
# and appends ONLY NEW results to database/data_price_usd_eur.csv.
# Output and logging adopt the requested CLI layout/style (info lines, progress bar, summary).
# Software Version 2.4
# NOTE for future maintainers (including AI):
# Every time you update this code, please increase the Software Version by +0.1

import requests
from decimal import Decimal
import csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
import sys
import math

TIMEOUT = 5  # seconds
PROGRESS_BAR_WIDTH = 40

# Minimal timezone abbreviation map for parsing (extend as needed)
TZ_OFFSETS = {
    "UTC": timedelta(hours=0),
    "CEST": timedelta(hours=2),
    "CET": timedelta(hours=1),
    "PDT": timedelta(hours=-7),
    "PST": timedelta(hours=-8),
    "EDT": timedelta(hours=-4),
    "EST": timedelta(hours=-5),
    "BST": timedelta(hours=1),
    "GMT": timedelta(hours=0),
}

def parse_ts_with_abbrev(ts_str: str) -> datetime:
    # Expect 'YYYY-MM-DD HH:MM:SS ZZZZ'
    parts = ts_str.rsplit(" ", 1)
    if len(parts) != 2:
        raise ValueError(f"Unsupported timestamp format: {ts_str}")
    dt_part, tz_abbr = parts[0], parts[1].strip()
    if tz_abbr not in TZ_OFFSETS:
        raise ValueError(f"Unknown timezone abbreviation: {tz_abbr} in '{ts_str}'")
    naive = datetime.strptime(dt_part, "%Y-%m-%d %H:%M:%S")
    aware = naive.replace(tzinfo=timezone(TZ_OFFSETS[tz_abbr]))
    return aware.astimezone(timezone.utc)

def fetch_rate_for_date_utc(utc_dt: datetime):
    date_str = utc_dt.strftime("%Y-%m-%d")
    # Primary: exchangerate.host
    try:
        url = f"https://api.exchangerate.host/{date_str}"
        resp = requests.get(url, params={"base": "USD", "symbols": "EUR"}, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        usd2eur = Decimal(str(data["rates"]["EUR"]))
        eur2usd = Decimal("1") / usd2eur
        return usd2eur, eur2usd, "exchangerate.host", date_str
    except Exception:
        pass
    # Fallback: frankfurter.app
    url = f"https://api.frankfurter.app/{date_str}"
    resp = requests.get(url, params={"from": "USD", "to": "EUR"}, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    usd2eur = Decimal(str(data["rates"]["EUR"]))
    eur2usd = Decimal("1") / usd2eur
    return usd2eur, eur2usd, "frankfurter.app", date_str

def read_existing_keys(out_csv: Path) -> set:
    keys = set()
    if not out_csv.exists():
        return keys
    with out_csv.open(mode="r", newline="") as f:
        reader = csv.DictReader(f)
        if "tx_hash" not in (reader.fieldnames or []):
            return keys
        for row in reader:
            key = (row.get("tx_hash") or "").strip()
            if key:
                keys.add(key)
    return keys

def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(mode="r", newline="") as f:
        # subtract header if present
        return max(sum(1 for _ in f) - 1, 0)

def save_rows(output_path: Path, header: list, rows: list):
    file_exists = output_path.exists()
    with output_path.open(mode="a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerows(rows)

def progress_bar(i: int, n: int):
    if n == 0:
        bar = " " * PROGRESS_BAR_WIDTH
        pct = 100
    else:
        pct = int((i / n) * 100)
        filled = math.floor((i / n) * PROGRESS_BAR_WIDTH)
        bar = "█" * filled + " " * (PROGRESS_BAR_WIDTH - filled)
    sys.stdout.write(f"\r[PROGRESS {pct}%] |{bar}|")
    sys.stdout.flush()
    if i == n:
        sys.stdout.write("\n")
        sys.stdout.flush()

def main():
    # Resolve paths relative to this file:
    base_dir = (Path(__file__).resolve().parent / ".." / "..").resolve()
    db_dir = base_dir / "database"
    db_dir.mkdir(parents=True, exist_ok=True)
    in_csv = db_dir / "data_decode_txs.csv"
    out_csv = db_dir / "data_price_usd_eur.csv"

    if not in_csv.exists():
        print(f"[INFO] Input CSV not found.")
        print(f"[INFO] Output: {out_csv}")
        return

    prev_written = count_rows(out_csv)

    # Read input CSV
    raw_rows = 0
    input_rows = []
    with in_csv.open(mode="r", newline="") as f:
        reader = csv.DictReader(f)
        if "tx_timestamp" not in reader.fieldnames or "tx_hash" not in reader.fieldnames:
            print("[INFO] Columns 'tx_hash' or 'tx_timestamp' not found in input CSV.")
            print(f"[INFO] Output: {out_csv}")
            return
        for row in reader:
            raw_rows += 1
            ts = (row.get("tx_timestamp") or "").strip()
            tx_hash = (row.get("tx_hash") or "").strip()
            if not ts or not tx_hash:
                continue
            try:
                utc_dt = parse_ts_with_abbrev(ts)
                input_rows.append((tx_hash, ts, utc_dt))
            except Exception:
                continue

    # Sort latest-first
    input_rows.sort(key=lambda x: x[2], reverse=True)
    fetched_in_range = len(input_rows)
    print(f"[INFO] Fetched {fetched_in_range} in range.")

    existing_keys = read_existing_keys(out_csv)
    already_decoded = len(existing_keys)
    already_fetched = sum(1 for tx_hash, _, _ in input_rows if tx_hash in existing_keys)

    if already_fetched == fetched_in_range:
        print("[INFO] No new rows to write.")
    else:
        print(f"[INFO] Will write {fetched_in_range - already_fetched} new row(s).")
    print(f"[INFO] Loaded {prev_written} previously decoded rows.")
    print(f"[INFO] Loaded {raw_rows} raw rows; {already_decoded} already decoded.")
    print(f"[INFO] Loaded {raw_rows} raw rows; {already_fetched} already fetched")

    to_process = [(tx_hash, ts, dt) for tx_hash, ts, dt in input_rows if tx_hash not in existing_keys]
    print(f"[INFO] Fetching price for {len(to_process)} transaction(s)...")

    header = [
        "tx_hash",
        "tx_timestamp",
        "tx_timestamp_utc",
        "usd_to_eur",
        "eur_to_usd",
        "forex_data_source",
        "forex_rate_date_utc",
    ]

    new_success = 0
    new_failed = 0
    to_append = []

    n = len(to_process)
    for idx, (tx_hash, ts, utc_dt) in enumerate(to_process, start=1):
        try:
            usd2eur, eur2usd, source, date_used = fetch_rate_for_date_utc(utc_dt)
            out_row = [
                tx_hash,
                ts,
                utc_dt.isoformat(),
                f"{usd2eur:.6f}",
                f"{eur2usd:.6f}",
                source,
                date_used,
            ]
            to_append.append(out_row)
            new_success += 1
        except Exception:
            new_failed += 1
        progress_bar(idx, n)

    if to_append:
        save_rows(out_csv, header, to_append)

    total_written = prev_written + new_success
    print(f"[INFO] Finished. New: {new_success} success, {new_failed} failed. Total written: {total_written} rows.")
    print(f"[INFO] Output: {out_csv}")

if __name__ == "__main__":
    main()

# Software Version 2.4
