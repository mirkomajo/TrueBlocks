# Software Version 1.2
# NOTE: Every time you (or a future AI) modify this script, 
#       please increment the Software Version above to keep track of updates.

import csv
import requests
import os
import time
import re
from datetime import datetime, timedelta, timezone

"""
Linea wallet raw fetcher (timeframe-enabled, no block lookups)
- Fetch last N normal transactions OR a timeframe from now
- Timeframes like: '1min', '2h', '3d', '1w', '1m' (month), '2mo', '1y'
- Absolute start time like '19.06.2025 11:30' (until now)
- Absolute range like '19.06.2025 11:30 to 21.06.2025 23:00' (NEW in v1.1)
- Appends to database/data_raw_txs.csv (skips dups by tx hash)
- Uses Etherscan v2 multi-chain endpoint with Linea (chain id 59144)
"""

# ========= CONFIG =========
WALLET_ADDRESS = "0x4e118f5a1ed501bd0b4eac76c8bd49ed1895bfc8"

API_KEY = os.getenv("ETHERSCAN_API_KEY", "IXW7N628V6G8G1M38MFJHTM7BZAV26SSVG")
BASE_URL = "https://api.etherscan.io/v2/api"  # Etherscan v2 multi-chain endpoint
CHAIN_ID = 59144  # Linea mainnet

# Script location: C:\TrueBlocks\modules\fetch
HERE = os.path.dirname(__file__)

# Go up two levels (to C:\TrueBlocks) then into "database"
ROOT_DIR = os.path.normpath(os.path.join(HERE, "..", ".."))
DATA_DIR = os.path.join(ROOT_DIR, "database")
os.makedirs(DATA_DIR, exist_ok=True)

RAW_CSV_FILENAME = os.path.join(DATA_DIR, "data_raw_txs.csv")
# ==========================

# Minimal console output
VERBOSE = False

# Local timezone
LOCAL_TZ = datetime.now().astimezone().tzinfo

session = requests.Session()

# ---- API helper

def _get(params: dict):
    params = {**params, "chainid": CHAIN_ID, "apikey": API_KEY}
    for attempt in range(4):
        r = session.get(BASE_URL, params=params, timeout=40)
        if r.status_code == 200:
            data = r.json()
            # handle rate limiting gently
            if (
                data.get("message") == "NOTOK"
                and isinstance(data.get("result"), str)
                and ("Max rate limit" in data["result"] or "rate limit" in data["result"].lower())
            ):
                time.sleep(0.8 + attempt * 0.4)
                continue
            return data
        time.sleep(0.4)
    r.raise_for_status()
    return r.json()

def fetch_last_normal_txs(wallet: str, limit: int):
    data = _get({
        "module": "account",
        "action": "txlist",
        "address": wallet,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": limit,
        "sort": "desc",
    })
    if data.get("status") == "1":
        return data["result"]
    if (data.get("message") or "").lower().startswith("no transactions found"):
        return []
    raise RuntimeError(f"API error: {data.get('message')} - {data.get('result')}")

def fetch_timeframe_txs(wallet: str, start_ts: int, end_ts: int):
    """
    Fetch transactions whose timestamp is in [start_ts, end_ts] WITHOUT using getblocknobytime.
    Strategy:
      - page through account.txlist sorted DESC (newest first)
      - collect items whose timeStamp is within [start_ts, end_ts]
      - stop once the last item on a page is older than start_ts
    """
    if start_ts > end_ts:
        start_ts, end_ts = end_ts, start_ts

    page = 1
    per_page = 1000  # conservative; increase if you know your key supports it
    collected = []

    while True:
        data = _get({
            "module": "account",
            "action": "txlist",
            "address": wallet,
            "startblock": 0,
            "endblock": 99999999,
            "page": page,
            "offset": per_page,
            "sort": "desc",  # newest first
        })

        # Handle "no transactions" gracefully
        if data.get("status") == "0" and (data.get("message") or "").lower().startswith("no transactions"):
            break
        if data.get("status") not in ("1",):
            # Some v2 responses return status 0 with an empty list; treat as done
            res = data.get("result")
            if isinstance(res, list) and not res:
                break
            raise RuntimeError(f"API error: {data.get('message')} - {data.get('result')}")

        page_items = data.get("result", [])
        if not page_items:
            break

        # Track oldest timestamp on this page to decide if we can stop paging
        oldest_ts_on_page = None

        for tx in page_items:
            try:
                ts = int(tx.get("timeStamp") or tx.get("timestamp") or 0)
            except Exception:
                ts = 0

            if oldest_ts_on_page is None or ts < oldest_ts_on_page:
                oldest_ts_on_page = ts

            if start_ts <= ts <= end_ts:
                collected.append(tx)

        # If the oldest tx on this page is already older than our start cutoff, we can stop.
        if oldest_ts_on_page is not None and oldest_ts_on_page < start_ts:
            break

        # If fewer than per_page items returned, no more pages.
        if len(page_items) < per_page:
            break

        page += 1
        time.sleep(0.2)  # be polite to the API

    # Return newest-first
    collected.sort(key=lambda x: int(x.get("timeStamp", x.get("timestamp", "0"))), reverse=True)
    return collected

# ---- Parsing

DUR_PATTERN = re.compile(
    r"^\s*(?:last\s+)?(\d+)\s*(min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks|mo|m|month|months|y|yr|yrs|year|years)\s*$",
    re.IGNORECASE,
)
COUNT_PATTERN = re.compile(r"^\s*(?:last\s+)?(\d+)(?:\s*txs?|\s*)\s*$", re.IGNORECASE)

ABSOLUTE_FORMATS = [
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d",
]

# NEW: patterns to detect explicit ranges
RANGE_SEPARATORS = [
    r"\s+to\s+",
    r"\s*-\s*",
    r"\s*–\s*",   # en-dash
    r"\s*—\s*",   # em-dash
    r"\s*\.\.\s*",  # ..
    r"\s*\.\.\.\s*",  # ...
]
RANGE_PREFIX = re.compile(r"^\s*from\s+", re.IGNORECASE)

def _months_ago(dt: datetime, n: int) -> datetime:
    """Subtract n calendar months from dt, keeping day bounds when possible."""
    year = dt.year
    month = dt.month - n
    while month <= 0:
        month += 12
        year -= 1
    # days per month
    days_in_month = [31, 29 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    day = min(dt.day, days_in_month)
    return dt.replace(year=year, month=month, day=day)

def _parse_absolute_dt(s: str) -> datetime | None:
    """Try all ABSOLUTE_FORMATS; assume LOCAL_TZ if naive."""
    for fmt in ABSOLUTE_FORMATS:
        try:
            dt = datetime.strptime(s.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LOCAL_TZ)
            return dt
        except Exception:
            continue
    return None

def _try_parse_range(s: str):
    """
    Try to parse an explicit 'start .. end' textual range.
    Returns (start_ts, end_ts) or None.
    """
    s = RANGE_PREFIX.sub("", s)  # strip leading 'from ' if present
    for sep in RANGE_SEPARATORS:
        parts = re.split(sep, s, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            start_raw, end_raw = parts[0].strip(), parts[1].strip()
            start_dt = _parse_absolute_dt(start_raw)
            end_dt = _parse_absolute_dt(end_raw)
            if start_dt and end_dt:
                st, et = int(start_dt.timestamp()), int(end_dt.timestamp())
                if st > et:
                    st, et = et, st
                return (st, et)
    return None

def parse_timeframe(s: str):
    """
    Returns:
      ("count", N)                 -> last N txs
      ("range", start_ts, end_ts)  -> unix timestamps inclusive
    """
    s = (s or "").strip()
    if not s:
        return ("count", 50)

    # 1) Explicit absolute range: "A to B", "A - B", "from A to B"
    rng = _try_parse_range(s)
    if rng:
        return ("range", rng[0], rng[1])

    # 2) Count like "50", "last 50"
    m = COUNT_PATTERN.match(s)
    if m:
        return ("count", int(m.group(1)))

    # 3) Duration like "1min", "2h", "3d", "1w", "1m"/"mo" (months), "1y"
    m = DUR_PATTERN.match(s)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        now = datetime.now(tz=LOCAL_TZ)
        end_ts = int(now.timestamp())

        if unit in ("min", "mins", "minute", "minutes"):
            start_dt = now - timedelta(minutes=n)
        elif unit in ("h", "hr", "hrs", "hour", "hours"):
            start_dt = now - timedelta(hours=n)
        elif unit in ("d", "day", "days"):
            start_dt = now - timedelta(days=n)
        elif unit in ("w", "week", "weeks"):
            start_dt = now - timedelta(weeks=n)
        elif unit in ("mo", "m", "month", "months"):
            # IMPORTANT: 'm' is months. Use 'min' for minutes.
            start_dt = _months_ago(now, n)
        elif unit in ("y", "yr", "yrs", "year", "years"):
            try:
                start_dt = now.replace(year=now.year - n)
            except ValueError:
                # Handle Feb 29 etc.
                start_dt = now - timedelta(days=365 * n)
        else:
            start_dt = now - timedelta(minutes=n)

        return ("range", int(start_dt.timestamp()), end_ts)

    # 4) Absolute date/time (from that time until now)
    dt = _parse_absolute_dt(s)
    if dt:
        end_ts = int(datetime.now(tz=LOCAL_TZ).timestamp())
        return ("range", int(dt.timestamp()), end_ts)

    # 5) If an integer slipped through without the pattern, treat as count
    if s.isdigit():
        return ("count", int(s))

    # 6) Default to last 50 if unrecognized
    if not VERBOSE:
        print("Unrecognized input; defaulting to last 50 transactions.", flush=True)
    return ("count", 50)

# ---- CSV helpers

def load_existing_hashes(csv_path: str):
    existing_hashes = set()
    fieldnames = None
    if os.path.exists(csv_path):
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                h = row.get("hash")
                if h:
                    existing_hashes.add(h)
    return existing_hashes, fieldnames

def append_rows(csv_path: str, rows: list, fieldnames: list | None):
    if not rows:
        return 0
    if fieldnames is None:
        keys = set()
        for r in rows:
            keys.update(r.keys())
        fieldnames = sorted(keys)

    mode = "a" if os.path.exists(csv_path) else "w"
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w" or os.stat(csv_path).st_size == 0:
            w.writeheader()
        for r in rows:
            row = {k: r.get(k, "") for k in fieldnames}
            w.writerow(row)
    return len(rows)

# ---- main

def main():
    print("[INFO] Starting raw fetch…", flush=True)
    prompt = (
        "[INFO] Timeframe? Examples:\n"
        "[INFO] 50 (txs) | 1min | 2h | 3d | 1w | 1m (month)\n"
        "[INFO] 19.06.2025 11:30\n"
        "[INFO] 19.06.2025 11:30 to 21.06.2025 23:00\n\n"
        "[INFO] Press Enter for default (last 50 txs): "
    )
    try:
        s = input(prompt)
    except EOFError:
        # Non-interactive: default to last 50
        s = ""
    kind, *vals = parse_timeframe(s)

    if kind == "count":
        limit = vals[0]
        if VERBOSE:
            print(f"Fetching last {limit} normal transactions for wallet {WALLET_ADDRESS}…", flush=True)
        txs = fetch_last_normal_txs(WALLET_ADDRESS, limit)
    else:
        start_ts, end_ts = vals
        if VERBOSE:
            st = datetime.fromtimestamp(start_ts, tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            et = datetime.fromtimestamp(end_ts, tz=LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"Fetching transactions for wallet {WALLET_ADDRESS} from {st} to {et}…", flush=True)
        txs = fetch_timeframe_txs(WALLET_ADDRESS, start_ts, end_ts)
        if not VERBOSE:
            print(f"[INFO] Fetched {len(txs)} in range.", flush=True)

    # Load existing raw to avoid duplicates
    existing_hashes, raw_fieldnames = load_existing_hashes(RAW_CSV_FILENAME)

    # Append fetched txs not already saved (by hash)
    txs_to_append = [tx for tx in txs if tx.get("hash") not in existing_hashes]
    if VERBOSE:
        print(f"Will append {len(txs_to_append)} new transactions (skip dups by hash).", flush=True)

    wrote = append_rows(RAW_CSV_FILENAME, txs_to_append, raw_fieldnames)

    if wrote:
        print(f"[INFO] Wrote {wrote} new rows to {RAW_CSV_FILENAME}", flush=True)
    else:
        print("[INFO] No new rows to write.", flush=True)

if __name__ == "__main__":
    main()
