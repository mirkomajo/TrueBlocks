"""
U00_csv_merger_tax_data.py
Version: 1.2.0

Merges three CSVs by tx_hash and writes a trimmed dataset with fixed schema:

tx_hash,tx_timestamp,type,amount_sent,amount_received,
rex_per_usdc,usdc_per_rex,onchaindata_source,
usd_to_eur,eur_to_usd,forex_data_source,forex_rate_date_utc
"""

from pathlib import Path
import pandas as pd

VERSION = "1.2.0"

OUTPUT_COLS = [
    "tx_hash",
    "tx_timestamp",
    "type",
    "amount_sent",
    "amount_received",
    "rex_per_usdc",
    "usdc_per_rex",
    "onchaindata_source",
    "usd_to_eur",
    "eur_to_usd",
    "forex_data_source",
    "forex_rate_date_utc",
]

def read_csv_str(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    return df

def main():
    print(f"Version: {VERSION}")

    repo_root = Path(__file__).resolve().parents[2]
    db_onchain = repo_root / "database" / "data_onchain"
    db_forex = repo_root / "database" / "data_forex"
    db_tax = repo_root / "database" / "data_tax_basis"

    txs_path = db_onchain / "data_decode_txs.csv"
    rex_path = db_onchain / "data_price_rex_usdc.csv"
    fx_path  = db_forex / "data_price_usd_eur.csv"
    out_path = db_tax / "data_tax_rex.csv"

    # Read inputs
    txs = read_csv_str(txs_path)
    rex = read_csv_str(rex_path)
    fx  = read_csv_str(fx_path)

    # Validate
    for name, df in [("data_decode_txs.csv", txs), ("data_price_rex_usdc.csv", rex), ("data_price_usd_eur.csv", fx)]:
        if "tx_hash" not in df.columns:
            raise ValueError(f"{name} is missing required column 'tx_hash'.")

    # Deduplicate
    txs = txs.drop_duplicates(subset=["tx_hash"], keep="last")
    rex = rex.drop_duplicates(subset=["tx_hash"], keep="last")
    fx  = fx.drop_duplicates(subset=["tx_hash"], keep="last")

    # Merge on tx_hash
    merged = txs.merge(rex, on="tx_hash", how="inner", suffixes=("", "_rex"))
    merged = merged.merge(fx,  on="tx_hash", how="inner", suffixes=("", "_fx"))

    # Ensure schema completeness
    for col in OUTPUT_COLS:
        if col not in merged.columns:
            merged[col] = ""

    final = merged[OUTPUT_COLS]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(out_path, index=False)

    print(f"Wrote {len(final)} rows to {out_path}")
    print(f"Version: {VERSION}")

if __name__ == "__main__":
    main()

# ------------------------------------------------------------
# Software Version (footer): 1.2.0
# ------------------------------------------------------------
