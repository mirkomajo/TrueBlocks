# A02_onchain_price_WETH_BTC.py
# Software Version 1.0
# Reads from C:\TrueBlocks\database\data_onchain\data_decode_txs.csv
# Writes to C:\TrueBlocks\database\data_onchain\data_price_weth_wbtc.csv
# - Only processes rows not already present in the output (by tx_hash)
# - Live CLI progress bar (no extra blank line before Finished)
# - Saves CSV sorted from latest to oldest by tx_timestamp
# - Output columns exactly:
#   tx_hash,tx_timestamp,block_time,type,from_address,to_address,amount_sent,amount_received,total_gas_eth,nft_transfere,weth_per_wbtc,wbtc_per_weth

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from decimal import Decimal, getcontext
import os
from typing import Tuple, Dict, Any, Optional, List, Union, Set
import csv
from datetime import datetime, timezone
from dateutil import tz, parser as dtparser
from dateutil.parser._parser import UnknownTimezoneWarning
from pathlib import Path
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --------------------------------- Precision ---------------------------------
getcontext().prec = 60

# ---------------------------------- Paths ------------------------------------
HERE = Path(__file__).resolve()
ROOT_DIR = HERE.parents[2]
DATA_DIR = ROOT_DIR / "database" / "data_onchain"
INPUT_CSV = DATA_DIR / "data_decode_txs.csv"
OUTPUT_CSV = DATA_DIR / "data_price_weth_wbtc.csv"
OUTPUT_TMP = OUTPUT_CSV.with_suffix(".csv.tmp")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------- Chain / Pool --------------------------------
LINEA_RPC_URL = os.environ.get("LINEA_RPC_URL", "https://rpc.linea.build")  # chainId 59144
POOL_ADDRESS = Web3.to_checksum_address("0xc0cd56e070e25913d631876218609f2191da1c2a")

# -------------------------------- Timezone -----------------------------------
USER_TZ = tz.gettz("Europe/Vienna")  # UTC+2 summer (CEST) / UTC+1 winter (CET)
TZINFOS = {
    "CEST": USER_TZ,
    "CET": USER_TZ,
    "UTC": tz.UTC,
    "GMT": tz.UTC,
}

# --------------------------------- Minimal ABIs ------------------------------
POOL_V3_META_ABI = [
    {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
]
POOL_V2_ABI = [
    {"inputs":[],"name":"getReserves","outputs":[
        {"internalType":"uint112","name":"reserve0","type":"uint112"},
        {"internalType":"uint112","name":"reserve1","type":"uint112"},
        {"internalType":"uint32","name":"blockTimestampLast","type":"uint32"}
    ],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
]
ERC20_ABI = [
    {"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
    {"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
]

Q96 = Decimal(2) ** 96

# ------------------------------- Web3 helpers --------------------------------
def connect() -> Web3:
    w3 = Web3(Web3.HTTPProvider(LINEA_RPC_URL, request_kwargs={"timeout": 30}))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if not w3.is_connected():
        raise RuntimeError("Could not connect to Linea RPC")
    return w3

def token_meta(w3: Web3, token_addr: str) -> Tuple[int, str]:
    t = w3.eth.contract(address=token_addr, abi=ERC20_ABI)
    decimals = t.functions.decimals().call()
    symbol = t.functions.symbol().call()
    return int(decimals), symbol

def price_from_sqrtPriceX96(sqrtPriceX96: int) -> Decimal:
    sp = Decimal(sqrtPriceX96)
    return (sp * sp) / (Q96 * Q96)

def read_v3_sqrtPriceX96_raw(w3: Web3, addr: Union[str, bytes], block_tag: Union[int, str]) -> int:
    if isinstance(addr, str):
        to_addr = Web3.to_checksum_address(addr)
    else:
        to_addr = addr
    selector = Web3.keccak(text="slot0()")[:4]  # 0x3850c7bd
    data = selector
    result = w3.eth.call({"to": to_addr, "data": data}, block_identifier=block_tag)
    if result is None or len(result) < 32:
        raise RuntimeError("slot0() returned no data")
    sqrt = int.from_bytes(result[0:32], byteorder="big")
    if sqrt == 0:
        raise RuntimeError("slot0() sqrtPriceX96 is zero")
    return sqrt

def detect_pool_type_and_meta(w3: Web3, addr: str):
    try:
        _ = read_v3_sqrtPriceX96_raw(w3, addr, "latest")
        pool_v3 = w3.eth.contract(address=addr, abi=POOL_V3_META_ABI)
        t0 = pool_v3.functions.token0().call()
        t1 = pool_v3.functions.token1().call()
        d0, s0 = token_meta(w3, t0)
        d1, s1 = token_meta(w3, t1)
        return "v3", {"t0": t0, "t1": t1, "d0": d0, "s0": s0, "d1": d1, "s1": s1}
    except Exception:
        pool_v2 = w3.eth.contract(address=addr, abi=POOL_V2_ABI)
        r0, r1, _ = pool_v2.functions.getReserves().call()
        t0 = pool_v2.functions.token0().call()
        t1 = pool_v2.functions.token1().call()
        d0, s0 = token_meta(w3, t0)
        d1, s1 = token_meta(w3, t1)
        return "v2", {"t0": t0, "t1": t1, "d0": d0, "s0": s0, "d1": d1, "s1": s1}

# ----------------------- Faster block finding with cache ----------------------
class BlockFinder:
    def __init__(self, w3: Web3):
        self.w3 = w3
        self._cache_by_minute: Dict[int, int] = {}
        self._latest_num = None
        self._latest_ts = None
        self.rpc_calls = 0
        self._lock = threading.Lock()

    def _get_block(self, num: int):
        b = self.w3.eth.get_block(num)
        with self._lock:
            self.rpc_calls += 1
        return b

    def _ensure_latest(self):
        if self._latest_num is None:
            self._latest_num = self.w3.eth.block_number
            latest = self._get_block(self._latest_num)
            self._latest_ts = int(latest.timestamp)

    def _binary_search(self, target_ts: int) -> int:
        self._ensure_latest()
        if self._latest_ts <= target_ts:
            return int(self._latest_num)
        low = 1
        high = self._latest_num
        while low <= high:
            mid = (low + high) // 2
            b = self._get_block(mid)
            ts = int(b.timestamp)
            if ts > target_ts:
                high = mid - 1
            else:
                nb = self._get_block(min(mid + 1, self._latest_num))
                nts = int(nb.timestamp)
                if nts > target_ts:
                    return int(mid)
                low = mid + 1
        return max(1, high)

    def find_before(self, target_ts: int, hint_block: Optional[int] = None, local_step_limit: int = 64) -> int:
        key = (target_ts // 60) * 60
        hit = self._cache_by_minute.get(key)
        if hit:
            return hit

        if hint_block:
            cur = max(1, hint_block)
            step = 0
            b = self._get_block(cur)
            while step < local_step_limit and int(b.timestamp) <= target_ts and cur < self.w3.eth.block_number:
                cur += 1
                b = self._get_block(cur)
                step += 1
            if int(b.timestamp) > target_ts:
                ans = max(1, cur - 1)
                self._cache_by_minute[key] = ans
                return ans

        ans = self._binary_search(target_ts)
        self._cache_by_minute[key] = ans
        return ans

# ---------------------------------- Ratios -----------------------------------
def get_ratios_at_block(w3: Web3, pool_type: str, meta: Dict[str, Any], block_tag: int) -> Dict[str, Any]:
    if pool_type == "v3":
        sqrtPriceX96 = read_v3_sqrtPriceX96_raw(w3, POOL_ADDRESS, block_tag)
        price1_per_0_raw = price_from_sqrtPriceX96(sqrtPriceX96)
        adj = Decimal(10) ** Decimal(meta["d0"] - meta["d1"])
        eth_per_wbtc = price1_per_0_raw * adj            # ETH per WBTC
        wbtc_per_eth = Decimal(1) / eth_per_wbtc         # WBTC per ETH
    else:
        pool = w3.eth.contract(address=POOL_ADDRESS, abi=POOL_V2_ABI)
        r0, r1, _ = pool.functions.getReserves().call(block_identifier=block_tag)
        r0 = Decimal(r0)
        r1 = Decimal(r1)
        eth_per_wbtc = (r1 / r0) * (Decimal(10) ** Decimal(meta["d0"] - meta["d1"]))
        wbtc_per_eth = Decimal(1) / eth_per_wbtc

    return {
        "ratios": {
            f"{meta['s0']}/{meta['s1']}": wbtc_per_eth,  # WBTC/ETH
            f"{meta['s1']}/{meta['s0']}": eth_per_wbtc,  # ETH/WBTC
        }
    }

# -------------------------------- CSV workflow -------------------------------
INPUT_FIELDS = [
    "tx_hash",
    "tx_timestamp",
    "block_time",
    "type",
    "from_address",
    "to_address",
    "amount_sent",
    "amount_received",
    "total_gas_eth",
    "nft_transfere",
]
OUTPUT_FIELDS = INPUT_FIELDS + ["weth_per_wbtc", "wbtc_per_weth"]

def parse_tx_time_to_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt_local = dtparser.parse(s, tzinfos=TZINFOS)
        if dt_local.tzinfo is None:
            dt_local = dt_local.replace(tzinfo=USER_TZ)  # assume Vienna if missing
        return dt_local.astimezone(timezone.utc)
    except UnknownTimezoneWarning:
        # Fallback just in case, though tzinfos should handle CEST/CET
        try:
            dt_local = dtparser.parse(s)
            if dt_local.tzinfo is None:
                dt_local = dt_local.replace(tzinfo=USER_TZ)
            return dt_local.astimezone(timezone.utc)
        except Exception:
            return None
    except Exception:
        return None

def parse_tx_time_for_sort(s: str) -> int:
    dt = parse_tx_time_to_utc(s)
    return int(dt.timestamp()) if dt else 0

def read_input_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            clean = {k: (r.get(k, "") or "") for k in INPUT_FIELDS}
            rows.append(clean)
    return rows

def read_existing_output(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            clean = {k: (r.get(k, "") or "") for k in OUTPUT_FIELDS}
            rows.append(clean)
    return rows

def existing_hashes(existing_rows: List[Dict[str, Any]]) -> Set[str]:
    return {(r.get("tx_hash") or "").lower() for r in existing_rows if r.get("tx_hash")}

def row_to_unix_ts(row: Dict[str, Any]) -> int:
    bt = (row.get("block_time") or "").strip()
    if bt:
        try:
            return int(bt)
        except Exception:
            pass
    dt_utc = parse_tx_time_to_utc(row.get("tx_timestamp", ""))
    if dt_utc is None:
        raise ValueError("Cannot resolve timestamp for row")
    return int(dt_utc.timestamp())

def write_all_atomic(path: Path, temp_path: Path, rows: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with temp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temp_path.replace(path)

# ------------------------------ Processing core ------------------------------
def extract_eth_wbtc_ratios(ratios: Dict[str, Decimal]) -> Tuple[Decimal, Decimal]:
    weth_per_wbtc = None
    wbtc_per_weth = None
    for pair, val in ratios.items():
        up = pair.upper()
        if (("ETH" in up or "WETH" in up) and "WBTC" in up):
            left, right = up.split("/")
            if ("ETH" in left or "WETH" in left) and "WBTC" in right:
                weth_per_wbtc = val
            if "WBTC" in left and ("ETH" in right or "WETH" in right):
                wbtc_per_weth = val
    if weth_per_wbtc is None and wbtc_per_weth is not None:
        weth_per_wbtc = Decimal(1) / wbtc_per_weth
    if wbtc_per_weth is None and weth_per_wbtc is not None:
        wbtc_per_weth = Decimal(1) / weth_per_wbtc
    if weth_per_wbtc is None or wbtc_per_weth is None:
        raise RuntimeError(f"Could not extract ETH/WBTC ratios from keys: {list(ratios.keys())}")
    return weth_per_wbtc, wbtc_per_weth

def process_row(w3: Web3, pool_type: str, meta: Dict[str, Any], bf: BlockFinder, row: Dict[str, Any], hint_block: Optional[int]):
    unix_ts = row_to_unix_ts(row)
    block_tag = bf.find_before(unix_ts, hint_block=hint_block)
    hist = get_ratios_at_block(w3, pool_type, meta, block_tag)
    weth_per_wbtc, wbtc_per_weth = extract_eth_wbtc_ratios(hist["ratios"])

    out_row = {k: row.get(k, "") for k in INPUT_FIELDS}
    out_row["weth_per_wbtc"] = f"{weth_per_wbtc:.18f}"
    out_row["wbtc_per_weth"] = f"{wbtc_per_weth:.18f}"
    out_row["_block_tag"] = block_tag
    out_row["_unix_ts"] = unix_ts
    out_row["_sort_ts"] = parse_tx_time_for_sort(out_row.get("tx_timestamp", ""))
    return out_row

# ------------------------------ CLI look & feel ------------------------------
_BAR_WIDTH = 40

def print_info(msg: str):
    print(f"[INFO] {msg}")

def _progress_line(done: int, total: int) -> str:
    pct = 0 if total == 0 else int((done / total) * 100)
    if pct > 100: pct = 100
    filled = int((_BAR_WIDTH * pct) / 100)
    bar = "█" * filled + " " * (_BAR_WIDTH - filled)
    return f"[PROGRESS {pct:>3}%] |{bar}|"

def print_progress(done: int, total: int):
    line = _progress_line(done, total)
    print("\r" + line, end="", flush=True)

def finish_progress():
    print("\r" + _progress_line(100, 100), flush=True)

# --------------------------------- Runner ------------------------------------
def run_incremental(workers: int = 4) -> Dict[str, Any]:
    # Load input + existing output
    raw_rows = read_input_rows(INPUT_CSV)
    existing_rows = read_existing_output(OUTPUT_CSV)
    done_hashes = existing_hashes(existing_rows)

    already = 0
    todo_rows: List[Dict[str, Any]] = []
    seen_this_batch: Set[str] = set()
    for r in raw_rows:
        h = (r.get("tx_hash") or "").lower()
        if not h:
            continue
        if h in done_hashes:
            already += 1
            continue
        if h in seen_this_batch:
            continue
        seen_this_batch.add(h)
        todo_rows.append(r)

    print_info("")
    print_info(f"Loaded {len(raw_rows)} raw rows; {already} already fetched")
    print_info(f"Fetching price for {len(todo_rows)} transaction(s)...")

    total_to_do = len(todo_rows)

    if total_to_do == 0:
        # Sort existing rows latest->oldest by tx_timestamp for consistency
        for r in existing_rows:
            r["_sort_ts"] = parse_tx_time_for_sort(r.get("tx_timestamp", ""))
        existing_rows.sort(key=lambda r: r["_sort_ts"], reverse=True)
        for r in existing_rows:
            r.pop("_sort_ts", None)
        finish_progress()
        write_all_atomic(OUTPUT_CSV, OUTPUT_TMP, existing_rows)
        print_info(f"Finished. New: 0 success, 0 failed. Total written: {len(existing_rows)} rows.")
        print_info(f"Output: {str(OUTPUT_CSV).lower()}")
        return {
            "new_success": 0,
            "new_failed": 0,
            "total_written": len(existing_rows)
        }

    # Prepare chain
    w3 = connect()
    pool_type, meta = detect_pool_type_and_meta(w3, POOL_ADDRESS)
    bf = BlockFinder(w3)

    # Resolve timestamps early for better caching
    resolved = []
    for r in todo_rows:
        try:
            ts = row_to_unix_ts(r)
            r["_unix_ts"] = ts
            r["_sort_ts"] = parse_tx_time_for_sort(r.get("tx_timestamp", ""))
            resolved.append(r)
        except Exception:
            # skip rows we can't timestamp
            pass
    resolved.sort(key=lambda x: x["_unix_ts"])
    total_to_do = len(resolved)

    produced = 0
    failed = 0
    out_rows: List[Dict[str, Any]] = []
    hint_for_chunk = None

    # initial progress line (0%)
    print_progress(0, total_to_do)

    start = time.time()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [ex.submit(process_row, w3, pool_type, meta, bf, r, hint_for_chunk) for r in resolved]
        for fut in as_completed(futures):
            try:
                out_row = fut.result()
                out_rows.append(out_row)
                produced += 1
                hint_for_chunk = out_row["_block_tag"]
            except Exception:
                failed += 1
            # update progress on every completion
            print_progress(produced + failed, total_to_do)

    # Clean internals for new rows
    for r in out_rows:
        r.pop("_unix_ts", None)
        r.pop("_block_tag", None)

    # Merge: existing + new (avoid duplicates)
    merged_rows = list(existing_rows)
    existing_set = existing_hashes(existing_rows)
    for r in out_rows:
        if (r.get("tx_hash") or "").lower() not in existing_set:
            merged_rows.append({k: r.get(k, "") for k in OUTPUT_FIELDS})

    # Sort latest -> oldest by tx_timestamp
    for r in merged_rows:
        r["_sort_ts"] = parse_tx_time_for_sort(r.get("tx_timestamp", ""))
    merged_rows.sort(key=lambda r: r["_sort_ts"], reverse=True)
    for r in merged_rows:
        r.pop("_sort_ts", None)

    # Write atomically
    write_all_atomic(OUTPUT_CSV, OUTPUT_TMP, merged_rows)

    # Final console (no extra blank line)
    finish_progress()
    elapsed = time.time() - start
    print_info(f"Finished. New: {produced - failed} success, {failed} failed. Total written: {len(merged_rows)} rows.")
    print_info(f"Output: {str(OUTPUT_CSV).lower()}")

    return {
        "new_success": produced - failed,
        "new_failed": failed,
        "total_written": len(merged_rows),
        "elapsed_sec": round(elapsed, 2),
    }

# ---------------------------------- Main -------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=min(4, (os.cpu_count() or 4)))
    args = parser.parse_args()
    run_incremental(workers=args.workers)

if __name__ == "__main__":
    main()

# A02_onchain_price_WETH_BTC.py
# Software Version 1.0