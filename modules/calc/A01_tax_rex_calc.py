# A01_tax_rex_calc.py
# EUR rounding to 2 decimals (ROUND_HALF_UP) for tax/reporting fields.

from __future__ import annotations
from pathlib import Path
import argparse
import pandas as pd
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

TAX_RATE_AT = 0.275  # 27.5%

def parse_amounts(field: str) -> dict[str, float]:
    if pd.isna(field) or not str(field).strip():
        return {}
    parts = [p.strip() for p in str(field).split(';') if p.strip()]
    out: dict[str, float] = {}
    for p in parts:
        try:
            amt_str, token = p.split()
            amt = float(amt_str)
            out[token] = out.get(token, 0.0) + amt
        except Exception:
            continue
    return out

def dict_to_str(d: dict[str, float]) -> str:
    if not d:
        return ""
    return "; ".join([f"{amt:+.12f} {tok}" for tok, amt in sorted(d.items())])

def money2(x) -> float:
    """Round to 2 decimals, HALF_UP, returned as float for CSV."""
    try:
        return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return 0.0

def load_data(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    df["timestamp"] = df["tx_timestamp"]
    df["sent_map"] = df["amount_sent"].apply(parse_amounts)
    df["recv_map"] = df["amount_received"].apply(parse_amounts)

    df["has_REX_sent"] = df["sent_map"].apply(lambda m: "REX" in m)
    df["has_REX_recv"] = df["recv_map"].apply(lambda m: "REX" in m)
    df["has_REX"] = df["has_REX_sent"] | df["has_REX_recv"]

    # LP reward inflow (taxable)
    df["rex_inflow_lp"] = df.apply(
        lambda r: (r["recv_map"].get("REX", 0.0) if r["type"] in ("add_liquidity", "remove_liquidity") else 0.0),
        axis=1,
    ).astype(float)

    # Per-tx valuation
    df["usdc_per_rex"] = pd.to_numeric(df["usdc_per_rex"], errors="coerce")
    df["usd_to_eur"]   = pd.to_numeric(df["usd_to_eur"], errors="coerce")
    df["eur_to_usd"]   = pd.to_numeric(df.get("eur_to_usd", 0.0), errors="coerce")

    df["usdc_value_at_inflow"] = df["rex_inflow_lp"] * df["usdc_per_rex"]
    df["eur_value_at_inflow"] = df["usdc_value_at_inflow"] * df["usd_to_eur"]

    # --- EUR rounding to 2 decimals (HALF_UP) ---
    df["eur_value_at_inflow"] = df["eur_value_at_inflow"].apply(money2)

    # Tax flags / amounts (swaps not taxable, LP inflow taxable)
    df["is_taxable"] = df["rex_inflow_lp"] > 0.0
    df["tax_basis_eur"] = df["eur_value_at_inflow"]  # already rounded to cents
    df["tax_rate"] = df["is_taxable"].apply(lambda x: TAX_RATE_AT if x else 0.0)
    df["tax_amount_eur"] = (df["tax_basis_eur"] * df["tax_rate"]).apply(money2)  # rounded per row

    # Swap diagnostics (not taxed here)
    def rex_sold(row):
        return -row["sent_map"].get("REX", 0.0) if row["type"] == "swap" and "REX" in row["sent_map"] else 0.0
    def rex_bought(row):
        return row["recv_map"].get("REX", 0.0) if row["type"] == "swap" and "REX" in row["recv_map"] else 0.0
    df["rex_sold_in_swap"] = df.apply(rex_sold, axis=1).astype(float)
    df["rex_bought_in_swap"] = df.apply(rex_bought, axis=1).astype(float)

    # Audit notes
    def note(row):
        if row["is_taxable"]:
            return "LP reward inflow (taxable at receipt)"
        if row["type"] == "swap":
            return "Swap (not taxable here; investment tax later)"
        return "Non-taxable"
    df["notes"] = df.apply(note, axis=1)

    # Human-readable parsed
    df["amount_sent_parsed"] = df["sent_map"].apply(dict_to_str)
    df["amount_received_parsed"] = df["recv_map"].apply(dict_to_str)

    return df

def summarize(df: pd.DataFrame) -> str:
    total_txs = int(len(df))
    rex_tx_count = int(df["has_REX"].sum())
    type_counts = df[df["has_REX"]].groupby("type").size().to_dict()
    for k in ("swap", "add_liquidity", "remove_liquidity", "other"):
        type_counts.setdefault(k, 0)
    total_rex_in = float(df["recv_map"].apply(lambda m: m.get("REX", 0.0)).sum())
    total_rex_out = -float(df["sent_map"].apply(lambda m: m.get("REX", 0.0)).sum())
    net_rex = total_rex_in - total_rex_out

    total_lp_usdc = float(df["usdc_value_at_inflow"].sum())
    total_lp_eur = float(df["eur_value_at_inflow"].sum())           # sum of per-row rounded EUR
    tax_total_eur = float(df["tax_amount_eur"].sum())               # sum of per-row rounded taxes
    net_after_tax_eur = total_lp_eur - tax_total_eur

    # Optional info: swaps (not taxed)
    from collections import defaultdict
    sold = defaultdict(lambda: {"rex_sold": 0.0, "asset_received": 0.0})
    bought = defaultdict(lambda: {"rex_bought": 0.0, "asset_spent": 0.0})
    for _, row in df[df["type"] == "swap"].iterrows():
        s, r = row["sent_map"], row["recv_map"]
        if "REX" in s:
            rex_amt = -s["REX"]
            for tok, amt in r.items():
                if tok == "REX": continue
                sold[tok]["rex_sold"] += rex_amt
                sold[tok]["asset_received"] += amt
        if "REX" in r:
            rex_amt = r["REX"]
            for tok, amt in s.items():
                if tok == "REX": continue
                bought[tok]["rex_bought"] += rex_amt
                bought[tok]["asset_spent"] += -amt

    lines = []
    lines.append(f"{rex_tx_count} Transactions with REX txs found from {total_txs} total txs loaded")
    lines.append(f"$REX transactions {type_counts.get('swap',0)} swaps, {type_counts.get('add_liquidity',0)} add liquid and {type_counts.get('remove_liquidity',0)} remove liquidity, {type_counts.get('other',0)} others")
    lines.append("")
    lines.append(f"Total In REX: {total_rex_in:.12f} REX")
    lines.append(f"Total Out REX: {total_rex_out:.12f} REX")
    lines.append(f"Difference In Out REX: {net_rex:.12f} REX")
    lines.append("")
    lines.append(f"VALUE TOTAL $REX AT INFLOW: {total_lp_usdc:.6f} USDC")
    lines.append(f"VALUE TOTAL $REX AT INFLOW: {total_lp_eur:.2f} €")
    lines.append("")
    lines.append("TAX 27.5% on LP inflow only")
    lines.append(f"TAX TO PAY: {tax_total_eur:.2f} €")
    lines.append(f"NET PROFIT (LP inflow - tax): {net_after_tax_eur:.2f} €")
    return "\n".join(lines)

def make_report_table(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "tx_hash","timestamp","type",
        "onchaindata_source","forex_data_source","forex_rate_date_utc",
        "amount_sent_parsed","amount_received_parsed",
        "usdc_per_rex","usd_to_eur","eur_to_usd",
        "rex_inflow_lp","rex_sold_in_swap","rex_bought_in_swap",
        "usdc_value_at_inflow","eur_value_at_inflow",
        "is_taxable","tax_basis_eur","tax_rate","tax_amount_eur",
        "notes",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = "" if c in ("notes","amount_sent_parsed","amount_received_parsed","onchaindata_source","forex_data_source","forex_rate_date_utc") else 0.0
    report_df = df[cols].copy()

    # Ensure numeric types & final EUR rounding (in case)
    num_cols = ["usdc_per_rex","usd_to_eur","eur_to_usd","rex_inflow_lp","rex_sold_in_swap","rex_bought_in_swap","usdc_value_at_inflow","eur_value_at_inflow","tax_basis_eur","tax_rate","tax_amount_eur"]
    for c in num_cols:
        report_df[c] = pd.to_numeric(report_df[c], errors="coerce").fillna(0.0)

    # Enforce 2-decimal strings for EUR columns in CSV output
    for c in ("eur_value_at_inflow","tax_basis_eur","tax_amount_eur"):
        report_df[c] = report_df[c].apply(money2)

    return report_df.sort_values(["timestamp","tx_hash"])

def main():
    here = Path(__file__).resolve()
    input_csv  = here.parents[2] / "database" / "data_tax_rex.csv"
    output_csv = here.parents[2] / "database" / "data_tax_report_rex.csv"

    ap = argparse.ArgumentParser(description="REX tax report (Austria): LP inflows taxed at receipt; swaps not taxed here. EUR rounded to 2 decimals.")
    ap.add_argument("--csv", type=Path, default=input_csv, help="Input CSV (default: ../../database/data_tax_rex.csv)")
    ap.add_argument("--out", type=Path, default=output_csv, help="Output CSV (default: ../../database/data_tax_report_rex.csv)")
    args = ap.parse_args()

    df = load_data(args.csv)
    report_df = make_report_table(df)
    report_df.to_csv(args.out, index=False, encoding="utf-8")

    print(summarize(df))
    print(f"\nSaved single tax report CSV to: {args.out}")

if __name__ == "__main__":
    main()
