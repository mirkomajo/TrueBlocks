# Software Version 1.0 
# Update Software Version with every update +0.1 (also for future AI)

import os
import re
import pandas as pd

# --- config (relative paths) ---
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))      # folder containing A00_rex_calc.py
ROOT_DIR   = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))  # go up to C:\TrueBlocks
DATA_DIR   = os.path.join(ROOT_DIR, "database")
os.makedirs(DATA_DIR, exist_ok=True)  # ensure the database folder exists

INPUT_CSV    = os.path.join(DATA_DIR, "data_decode_txs.csv")
FILTERED_CSV = os.path.join(DATA_DIR, "rex_filter_txs.csv")
SUMMARY_CSV  = os.path.join(DATA_DIR, "rex_summary.csv")
TOKEN = "REX"

# --- helpers ---
TOKEN_RE = re.compile(r'([+-]?\d+(?:\.\d+)?)\s*([A-Za-z0-9_]+)')

def parse_amounts(cell: str):
    if not isinstance(cell, str) or not cell.strip():
        return []
    parts = []
    for amt, sym in TOKEN_RE.findall(cell):
        try:
            parts.append((float(amt), sym))
        except ValueError:
            pass
    return parts

def has_token(cell: str, token: str) -> bool:
    return any(sym == token for _, sym in parse_amounts(cell))

def sum_token(row, token: str, field: str) -> float:
    total = 0.0
    for amt, sym in parse_amounts(row.get(field, "")):
        if sym == token:
            total += amt
    return total

def fmt12(x: float) -> str:
    return f"{x:.12f}"

# --- load input ---
df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
required_cols = {"tx_hash", "tx_timestamp", "block_time", "type", "amount_sent", "amount_received"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"Input CSV is missing required columns: {', '.join(sorted(missing))}")

# --- filter rows involving REX ---
mask_rex = df["amount_sent"].apply(lambda c: has_token(c, TOKEN)) | df["amount_received"].apply(lambda c: has_token(c, TOKEN))
df_rex = df[mask_rex].copy()

# --- append/dedup into rex_filter_txs.csv ---
if os.path.exists(FILTERED_CSV):
    df_existing = pd.read_csv(FILTERED_CSV, dtype=str).fillna("")
    combined = pd.concat([df_existing, df_rex], ignore_index=True)
else:
    combined = df_rex

combined = combined.drop_duplicates(subset=["tx_hash"], keep="first").reset_index(drop=True)
combined.to_csv(FILTERED_CSV, index=False)

# --- empty case ---
if combined.empty:
    print("")  # blank space instead of banner
    print("FIRST TX TIME")
    print("")
    print("")
    print("TOTAL YIELD")
    print(f"{fmt12(0.0)} ${TOKEN} ")
    print("")
    print("Total out (all types) ")
    print(f"{fmt12(0.0)} ${TOKEN} ")
    print("")
    print("Total swapped out (swaps)")
    print(f"{fmt12(0.0)} ${TOKEN}")
    print("")
    print("TOTAL LEFT ")
    print(f"{fmt12(0.0)} ${TOKEN} ")
    print("")
    print("REALIZED:")
    print("(none)")
    pd.DataFrame([{
        "total_yield": 0.0,
        "total_rex_out_all_types": 0.0,
        "total_rex_swapped_out": 0.0,
        "total_rex_left": 0.0,
        "first_tx_time": ""
    }]).to_csv(SUMMARY_CSV, index=False)
    raise SystemExit(0)

# --- earliest tx time (by block_time; fallback to tx_timestamp) ---
bt_numeric = pd.to_numeric(combined["block_time"], errors="coerce")
if bt_numeric.notna().any():
    first_idx = bt_numeric.idxmin()
else:
    ts_parsed = pd.to_datetime(combined["tx_timestamp"], errors="coerce")
    first_idx = ts_parsed.idxmin()
first_tx_time_str = str(combined.loc[first_idx, "tx_timestamp"])

# --- calculations ---
combined["rex_in"]  = combined.apply(lambda r: sum_token(r, TOKEN, "amount_received"), axis=1)
combined["rex_out"] = combined.apply(lambda r: sum_token(r, TOKEN, "amount_sent"), axis=1)

total_rex_in  = combined["rex_in"].sum()
total_rex_out = -combined["rex_out"].sum()  # flip sign to positive
rex_left      = total_rex_in - total_rex_out

# swaps: REX sent -> non-REX received (breakdown)
is_rex_swap_out = (combined["type"].str.lower() == "swap") & combined["amount_sent"].apply(lambda c: has_token(c, TOKEN))
recv_breakdown = {}
for _, row in combined[is_rex_swap_out].iterrows():
    for amt, sym in parse_amounts(row["amount_received"]):
        if sym != TOKEN:
            recv_breakdown[sym] = recv_breakdown.get(sym, 0.0) + amt

total_rex_swapped_out = 0.0
for _, row in combined[is_rex_swap_out].iterrows():
    for amt, sym in parse_amounts(row["amount_sent"]):
        if sym == TOKEN:
            total_rex_swapped_out += -amt

# --- CLI output (blank top line, no banner) ---
print("")  # keep the top blank space
print("FIRST TX TIME")
print(f"{first_tx_time_str}")
print("")
print("")
print("TOTAL YIELD")
print(f"{fmt12(total_rex_in)} ${TOKEN} ")
print("")
print("Total out (all types) ")
print(f"{fmt12(total_rex_out)} ${TOKEN} ")
print("")
print("Total swapped out (swaps)")
print(f"{fmt12(total_rex_swapped_out)} ${TOKEN}")
print("")
print("TOTAL LEFT ")
print(f"{fmt12(rex_left)} ${TOKEN} ")
print("")
print("REALIZED:")
if recv_breakdown:
    for sym in sorted(recv_breakdown):
        print(f"{sym} {fmt12(recv_breakdown[sym])}")
else:
    print("(none)")

# --- summary csv (includes per-token breakdown) ---
summary = {
    "total_yield": total_rex_in,
    "total_rex_out_all_types": total_rex_out,
    "total_rex_swapped_out": total_rex_swapped_out,
    "total_rex_left": rex_left,
    "first_tx_time": first_tx_time_str,
}
for sym, amt in recv_breakdown.items():
    summary[f"recv_{sym}"] = amt

pd.DataFrame([summary]).to_csv(SUMMARY_CSV, index=False)
