# A01_decode_txs.py — Linea (CHAIN_ID=59144)
# Software Version 1.1  (NFT name decode + 1155 batch + RPC fallback)

import os, csv, time, string, sys, json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from decimal import Decimal, getcontext
from eth_abi import decode as abi_decode
from eth_utils import keccak, to_checksum_address
from pathlib import Path

# --------------------------- Paths / Config ---------------------------
HERE = os.path.dirname(__file__)
ROOT_DIR = os.path.normpath(os.path.join(HERE, "..", ".."))
CONFIG_DIR = os.path.join(ROOT_DIR, "config")

try:
    with open(os.path.join(CONFIG_DIR, "config.json"), "r", encoding="utf-8") as f:
        WALLET_ADDRESS = json.load(f).get("wallet_address", "").lower()
except Exception:
    WALLET_ADDRESS = ""

def load_api_key() -> str:
    key_path = os.path.join(CONFIG_DIR, "api_key_etherscan.txt")
    try:
        with open(key_path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except FileNotFoundError:
        return os.getenv("ETHERSCAN_API_KEY", "")

API_KEY = load_api_key()
BASE_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 59144  # Linea mainnet

DATA_DIR = os.path.join(ROOT_DIR, "database", "data_onchain")
os.makedirs(DATA_DIR, exist_ok=True)
RAW_CSV = Path(DATA_DIR) / "data_raw_txs.csv"
OUT_CSV_DECODED = Path(DATA_DIR) / "data_decode_txs.csv"

AT_TZ = ZoneInfo("Europe/Vienna")

LINEA_RPCS = [
    "https://rpc.linea.build",
    "https://linea.drpc.org",
    "https://1rpc.io/linea",
]

getcontext().prec = 50
session = requests.Session()
VERBOSE_DETAIL = False

def log_info(msg: str): print(f"[INFO] {msg}")
def log_tx(idx: int, txh: str, tx_type: str, sent: str, recv: str, gas: str):
    if not VERBOSE_DETAIL: return
    short = txh[:10] + "..."
    print(f"[TX#{idx}] {short} type={tx_type} sent={(sent or '-')} recv={(recv or '-')} gas={gas}")
def log_fail(idx: int, txh: str, gas: str):
    if not VERBOSE_DETAIL: return
    short = txh[:10] + "..."
    print(f"[FAIL#{idx}] {short} type=failed sent=- recv=- gas={gas}")

def print_progress(current: int, total: int, width: int = 36):
    if total <= 0: return
    current = max(0, min(current, total))
    filled = int(width * current / total)
    bar = "█" * filled + "-" * (width - filled)
    pct = int(100 * current / total)
    sys.stdout.write(f"\r[PROGRESS {pct}%] |{bar}|")
    sys.stdout.flush()
    if current == total: print()

# --------------------------- API helpers ---------------------------
def _get(params: dict):
    params = {**params, "chainid": CHAIN_ID, "apikey": API_KEY}
    for attempt in range(6):
        r = session.get(BASE_URL, params=params, timeout=40)
        if r.status_code == 200:
            j = r.json()
            if j.get("message") == "NOTOK" and isinstance(j.get("result"), str) and "Max rate limit" in j["result"]:
                time.sleep(0.6 * (attempt + 1)); continue
            return j
        time.sleep(0.4 * (attempt + 1))
    r.raise_for_status()
    return r.json()

def rpc(method_params: dict):
    j = _get({"module": "proxy", **method_params})
    return j.get("result") if isinstance(j, dict) else None

def get_tx_receipt(txhash: str):
    for attempt in range(6):
        res = rpc({"action": "eth_getTransactionReceipt", "txhash": txhash})
        if isinstance(res, dict): return res
        time.sleep(0.6 * (attempt + 1))
    return None

def get_block_by_hash(block_hash: str):
    return rpc({"action": "eth_getBlockByHash", "tag": block_hash, "boolean": "false"})

def get_block_by_number(block_hex: str):
    return rpc({"action": "eth_getBlockByNumber", "tag": block_hex, "boolean": "false"})

def get_balance(addr: str, tag_hex: str) -> str:
    return rpc({"action": "eth_getBalance", "address": addr, "tag": tag_hex}) or "0x0"

def hex_tag(n: int) -> str: return hex(max(n, 0))

def eth_call_proxy(to: str, data: str) -> str | None:
    return rpc({"action": "eth_call", "to": to, "data": data, "tag": "latest"})

def rpc_direct(url: str, method: str, params: list):
    try:
        r = requests.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=25)
        if r.status_code != 200: return None
        j = r.json()
        if "error" in j: return None
        return j.get("result")
    except Exception:
        return None

def eth_call_direct(to: str, data: str) -> str | None:
    for url in LINEA_RPCS:
        res = rpc_direct(url, "eth_call", [{"to": to, "data": data}, "latest"])
        if isinstance(res, str) and is_hex_data(res):
            return res
    return None

def safe_eth_call(to: str, data: str) -> str | None:
    # try proxy first (cheap), then real RPCs
    try:
        res = eth_call_proxy(to, data)
        if is_hex_data(res or ""): return res
    except Exception:
        pass
    return eth_call_direct(to, data)

# We keep ERC-20 metadata helper (works for NFTs too)
def fetch_tokentx_metadata(txhash: str) -> dict:
    out = {}
    j = _get({"module": "account", "action": "tokentx", "txhash": txhash, "page": 1, "offset": 1000, "sort": "asc"})
    if j.get("status") == "1":
        for it in j.get("result", []):
            addr = (it.get("contractAddress") or "").lower()
            sym = (it.get("tokenSymbol") or "").strip()
            try: dec = int(it.get("tokenDecimal") or "0")
            except Exception: dec = 0
            if addr: out[addr] = {"symbol": sym or "UNKNOWN", "decimals": dec}
    return out

# NOTE: removed broken tokennfttx(txhash) enrichment; we’ll resolve NFT names via direct contract calls.

# --------------------------- hex/topic utils ---------------------------
_HEXCHARS = set(string.hexdigits)
def is_hex_data(s: str) -> bool:
    if not isinstance(s, str) or not s.startswith("0x"): return False
    payload = s[2:]
    if len(payload) == 0: return True
    if len(payload) % 2 != 0: return False
    return all(c in _HEXCHARS for c in payload)

def address_from_topic(topic_hex: str) -> str:
    try:
        s = "0x" + topic_hex[-40:]
        return to_checksum_address(s).lower()
    except Exception:
        return ""

def h2i(x: str | None) -> int:
    if not x or x == "0x": return 0
    return int(x, 16)

def decode_address(addr: str | None) -> str:
    s = (addr or "").strip()
    if not s or not s.startswith("0x") or len(s) != 42: return ""
    try: return to_checksum_address(s)
    except Exception: return ""

# --------------------------- topics ---------------------------
TOPIC_TRANSFER              = "0x" + keccak(text="Transfer(address,address,uint256)").hex()              # ERC20/721
TOPIC_TRANSFER_SINGLE       = "0x" + keccak(text="TransferSingle(address,address,address,uint256,uint256)").hex()  # ERC1155
TOPIC_TRANSFER_BATCH        = "0x" + keccak(text="TransferBatch(address,address,address,uint256[],uint256[])").hex() # ERC1155 batch
TOPIC_WETH_DEPOSIT          = "0x" + keccak(text="Deposit(address,uint256)").hex()
TOPIC_WETH_WITHDRAWAL       = "0x" + keccak(text="Withdrawal(address,uint256)").hex()

# Uniswap-ish topics for classification
TOPIC_SWAP_V3    = "0x" + keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
TOPIC_MINT_V3    = "0x" + keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
TOPIC_BURN_V3    = "0x" + keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()
TOPIC_INC_LIQ_V3 = "0x" + keccak(text="IncreaseLiquidity(uint256,uint128,uint256,uint256)").hex()
TOPIC_DEC_LIQ_V3 = "0x" + keccak(text="DecreaseLiquidity(uint256,uint128,uint256,uint256)").hex()
TOPIC_SWAP_V2    = "0x" + keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
TOPIC_MINT_V2    = "0x" + keccak(text="Mint(address,uint256,uint256)").hex()
TOPIC_BURN_V2    = "0x" + keccak(text="Burn(address,uint256,uint256,address)").hex()

# --------------------------- ERC-20 / NFT metadata ---------------------------
SEL_SYMBOL   = "0x95d89b41"
SEL_DECIMALS = "0x313ce567"
SEL_NAME     = "0x06fdde03"

SYMBOL_ALIASES = { "wrapped btc": "WBTC", "wbtc.e": "WBTC" }
DECIMALS_BY_SYMBOL = { "WBTC": 8, "USDC": 6, "USDT": 6, "DAI": 18, "WETH": 18, "ETH": 18 }

_symbol_cache: dict[str, str] = {}
_dec_cache: dict[str, int] = {}
_name_cache: dict[str, str] = {}

def decode_string_return(hexdata: str) -> str | None:
    if not is_hex_data(hexdata) or hexdata == "0x": return None
    raw = bytes.fromhex(hexdata[2:])
    try:
        return abi_decode(["string"], raw)[0]
    except Exception:
        try:
            b = abi_decode(["bytes32"], raw)[0]
            return b.rstrip(b"\x00").decode("utf-8", errors="replace")
        except Exception:
            return None

def normalize_symbol(sym: str) -> str:
    s = (sym or "").strip()
    if not s: return "UNKNOWN"
    alias = SYMBOL_ALIASES.get(s.lower())
    return alias or s

def token_name(addr: str) -> str | None:
    a = to_checksum_address(addr).lower()
    if a in _name_cache: return _name_cache[a]
    res = safe_eth_call(a, SEL_NAME)
    if is_hex_data(res or ""):
        s = decode_string_return(res)
        if s and s.isprintable():
            _name_cache[a] = s.strip()
            return _name_cache[a]
    # fallback to symbol if name unavailable
    sym = erc20_symbol(a)
    if sym and sym != "UNKNOWN":
        _name_cache[a] = sym
        return sym
    return None

def erc20_symbol(addr: str, meta_hint: dict | None = None) -> str:
    a = addr.lower()
    if meta_hint and a in meta_hint and meta_hint[a].get("symbol"):
        return normalize_symbol(meta_hint[a]["symbol"])
    if a in _symbol_cache: return _symbol_cache[a]
    for sel in (SEL_SYMBOL, SEL_NAME):
        res = safe_eth_call(addr, sel)
        if not is_hex_data(res or ""): continue
        s = decode_string_return(res)
        if s and s.isprintable():
            s = normalize_symbol(s)
            _symbol_cache[a] = s
            return s
    _symbol_cache[a] = "UNKNOWN"
    return "UNKNOWN"

def erc20_decimals(addr: str, meta_hint: dict | None = None, sym_hint: str | None = None) -> int:
    a = addr.lower()
    if meta_hint and a in meta_hint:
        d = int(meta_hint[a].get("decimals") or 0)
        if d > 0: return d
    if a in _dec_cache: return _dec_cache[a]
    res = safe_eth_call(addr, SEL_DECIMALS)
    if is_hex_data(res or "") and res != "0x":
        try:
            d = int(abi_decode(["uint8"], bytes.fromhex(res[2:]))[0])
            _dec_cache[a] = d
            return d
        except Exception:
            pass
    if sym_hint:
        d2 = DECIMALS_BY_SYMBOL.get(sym_hint.upper())
        if isinstance(d2, int):
            _dec_cache[a] = d2
            return d2
    _dec_cache[a] = 18
    return 18

# --------------------------- formatting ---------------------------
def fmt_amount(value_wei: int, decimals: int, min_dp: int = 2, max_dp: int = 12) -> str:
    if value_wei == 0: return f"{Decimal(0):.{min_dp}f}"
    q = Decimal(value_wei) / (Decimal(10) ** Decimal(decimals))
    s = f"{q:.{max_dp}f}"
    whole, dot, frac = s.partition(".")
    trimmed = frac.rstrip("0")
    if len(trimmed) < min_dp:
        trimmed = (trimmed + "0" * min_dp)[:min_dp]
    return whole + (dot + trimmed if trimmed else "")

def format_token_display_signed(delta_wei: int, decimals: int, symbol: str) -> str:
    sign = "-" if delta_wei < 0 else "+"
    return f"{sign}{fmt_amount(abs(delta_wei), decimals)} {symbol}"

def format_nft_signed(symbol_or_name: str, token_id: int, qty: int) -> str:
    sign = "-" if qty < 0 else "+"
    label = f"{symbol_or_name}#{token_id}"
    return f"{sign}{label}" if abs(qty) == 1 else f"{sign}{label} x {abs(qty)}"

def wei_str_eth(wei: int) -> str:
    return fmt_amount(wei, 18, min_dp=5, max_dp=8)

# --------------------------- classify ---------------------------
def classify_from_topics(logs: list[dict]) -> str | None:
    t0s = {(lg.get("topics") or [""])[0] for lg in logs if lg.get("topics")}
    if any(t in t0s for t in (TOPIC_SWAP_V3, TOPIC_SWAP_V2)): return "swap"
    if any(t in t0s for t in (TOPIC_MINT_V3, TOPIC_INC_LIQ_V3, TOPIC_MINT_V2)): return "add_liquidity"
    if any(t in t0s for t in (TOPIC_BURN_V3, TOPIC_DEC_LIQ_V3, TOPIC_BURN_V2)): return "remove_liquidity"
    return None

# --------------------------- core delta ---------------------------
def compute_wallet_deltas(row_tx: dict, rcpt: dict, wallet: str, meta_hint: dict):
    """
    Returns:
      erc20_eth: dict[str|'eth' -> int delta] (signed; +recv, -sent)
      nft_moves: list[(contract, tokenId, deltaQty)]
    """
    wallet = (wallet or "").lower().strip()
    erc20_eth: dict[str, int] = {}
    nft_moves: list[tuple[str, int, int]] = []

    # OUTER msg.value
    eth_log_delta = 0
    try: outer_val = int(row_tx.get("value") or "0")
    except Exception: outer_val = 0
    if wallet and outer_val > 0:
        if (row_tx.get("from") or "").lower() == wallet: eth_log_delta -= outer_val
        if (row_tx.get("to") or "").lower() == wallet:   eth_log_delta += outer_val

    # Logs: ERC20/721/1155 + WETH
    for lg in (rcpt.get("logs") or []):
        t = lg.get("topics") or []
        if not t: continue
        t0 = t[0]
        addr = (lg.get("address") or "").lower()

        if t0 == TOPIC_TRANSFER and len(t) >= 3:
            frm = address_from_topic(t[1])
            to  = address_from_topic(t[2])
            if len(t) >= 4:
                # ERC-721
                token_id = h2i(t[3])
                if wallet:
                    if frm == wallet: nft_moves.append((addr, token_id, -1))
                    if to  == wallet: nft_moves.append((addr, token_id, +1))
            else:
                # ERC-20
                data_hex = lg.get("data", "0x")
                if is_hex_data(data_hex) and data_hex != "0x":
                    amt = int(data_hex, 16)
                    if wallet and amt > 0:
                        if frm == wallet: erc20_eth[addr] = erc20_eth.get(addr, 0) - amt
                        if to  == wallet: erc20_eth[addr] = erc20_eth.get(addr, 0) + amt

        elif t0 == TOPIC_TRANSFER_SINGLE and len(t) >= 4:
            data_hex = (lg.get("data") or "0x")[2:]
            if len(data_hex) >= 128:
                token_id = int(data_hex[0:64], 16)
                qty      = int(data_hex[64:128], 16)
                frm = address_from_topic(t[2]); to = address_from_topic(t[3])
                if wallet and qty > 0:
                    if frm == wallet: nft_moves.append((addr, token_id, -qty))
                    if to  == wallet: nft_moves.append((addr, token_id, +qty))

        elif t0 == TOPIC_TRANSFER_BATCH and len(t) >= 4:
            # ERC-1155 batch: data encodes (uint256[] ids, uint256[] values)
            data_hex = (lg.get("data") or "0x")
            if is_hex_data(data_hex) and data_hex != "0x":
                try:
                    ids, values = abi_decode(["uint256[]","uint256[]"], bytes.fromhex(data_hex[2:]))
                except Exception:
                    ids, values = [], []
                frm = address_from_topic(t[2]); to = address_from_topic(t[3])
                if wallet and len(ids) == len(values):
                    for token_id, qty in zip(ids, values):
                        q = int(qty)
                        if q <= 0: continue
                        if frm == wallet: nft_moves.append((addr, int(token_id), -q))
                        if to  == wallet: nft_moves.append((addr, int(token_id), +q))

        elif t0 == TOPIC_WETH_WITHDRAWAL and len(t) >= 2:
            who = address_from_topic(t[1]); amt = h2i(lg.get("data"))
            if wallet and who == wallet and amt > 0: eth_log_delta += amt

        elif t0 == TOPIC_WETH_DEPOSIT and len(t) >= 2:
            who = address_from_topic(t[1]); amt = h2i(lg.get("data"))
            if wallet and who == wallet and amt > 0: eth_log_delta -= amt

    # Internals
    try:
        internals = _get({"module": "account", "action": "txlistinternal", "txhash": row_tx["hash"], "page": 1, "offset": 1000, "sort": "asc"})
        if internals.get("status") == "1":
            for itx in internals.get("result", []):
                frm = (itx.get("from") or "").lower()
                to  = (itx.get("to") or "").lower()
                val = int(itx.get("value") or "0")
                if not wallet or val <= 0: continue
                if to  == wallet: eth_log_delta += val
                if frm == wallet: eth_log_delta -= val
    except Exception:
        pass

    if eth_log_delta != 0:
        erc20_eth["eth"] = erc20_eth.get("eth", 0) + eth_log_delta

    # trace fallback
    if "eth" not in erc20_eth:
        traced = trace_eth_delta(row_tx["hash"], wallet)
        if isinstance(traced, int) and traced != 0: erc20_eth["eth"] = traced

    # balance-delta fallback
    if "eth" not in erc20_eth:
        try:
            bn = int(row_tx.get("blockNumber") or "0")
            before_hex = hex_tag(bn - 1 if bn > 0 else 0)
            after_hex  = hex_tag(bn)
            bal_before = h2i(get_balance(wallet, before_hex))
            bal_after  = h2i(get_balance(wallet, after_hex))
            eth_delta = bal_after - bal_before
            if (row_tx.get("from") or "").lower() == wallet:
                gas_used = h2i(rcpt.get("gasUsed"))
                eff_price = h2i(rcpt.get("effectiveGasPrice")) or int(row_tx.get("gasPrice") or "0")
                gas_cost = gas_used * eff_price if (gas_used and eff_price) else 0
                eth_delta += gas_cost
            if eth_delta != 0: erc20_eth["eth"] = eth_delta
        except Exception:
            pass

    return erc20_eth, nft_moves

# --------------------------- stringify ---------------------------
def build_ft_strings(erc20_eth: dict, meta_hint: dict):
    sent_items, recv_items = [], []
    for key, delta in erc20_eth.items():
        if delta == 0: continue
        if key == "eth":
            s = format_token_display_signed(delta, 18, "ETH")
        else:
            checksum = to_checksum_address(key)
            sym = erc20_symbol(checksum, meta_hint)
            dec = erc20_decimals(checksum, meta_hint, sym_hint=sym)
            s = format_token_display_signed(delta, dec, sym)
        (sent_items if delta < 0 else recv_items).append(s)
    sent_str = "; ".join(sent_items) if sent_items else ""
    recv_str = "; ".join(recv_items) if recv_items else ""
    return sent_str, recv_str

def build_nft_field(nft_moves: list, meta_hint: dict) -> str:
    if not nft_moves: return ""
    parts = []
    for (contract, token_id, qty) in nft_moves:
        checksum = to_checksum_address(contract)
        # Prefer collection NAME; fallback to SYMBOL; finally "UNKNOWN"
        nm = token_name(checksum) or erc20_symbol(checksum, meta_hint) or "UNKNOWN"
        parts.append(format_nft_signed(nm, token_id, qty))
    return "; ".join(parts)

# --------------------------- traces ---------------------------
def trace_eth_delta(txhash: str, wallet: str) -> int | None:
    wl = (wallet or "").lower()
    for url in LINEA_RPCS:
        res = rpc_direct(url, "trace_transaction", [txhash])
        if not isinstance(res, list): continue
        delta = 0
        for tr in res:
            if tr.get("type") != "call": continue
            act = tr.get("action", {})
            value_hex = act.get("value") or "0x0"
            try: value = int(value_hex, 16)
            except Exception: value = 0
            if value == 0: continue
            frm = (act.get("from") or "").lower()
            to  = (act.get("to") or "").lower()
            if wl and to  == wl: delta += value
            if wl and frm == wl: delta -= value
        return delta
    return None

# --------------------------- decode one ---------------------------
def classify_from_balances(erc20_eth: dict) -> str:
    outs = sum(1 for v in erc20_eth.values() if v < 0)
    ins  = sum(1 for v in erc20_eth.values() if v > 0)
    if outs >= 1 and ins >= 1: return "swap"
    if outs >= 1 and ins == 0: return "add_liquidity"
    if outs == 0 and ins >= 1: return "remove_liquidity"
    return "unknown"

def decode_one_from_row(row: dict, already_known: set, idx_success: int, idx_failed: int):
    txh = row.get("hash") or row.get("tx_hash")
    if not txh or txh in already_known: return None

    from_addr = decode_address(row.get("from"))
    to_addr   = decode_address(row.get("to"))

    rcpt = get_tx_receipt(txh)
    if not isinstance(rcpt, dict): return None

    status_hex = str(rcpt.get("status") or "").lower()
    is_failed = (status_hex == "0x0")

    try: ts_unix = int(row.get("timeStamp") or "0")
    except Exception: ts_unix = 0
    if ts_unix == 0:
        block_hash = row.get("blockHash")
        block = get_block_by_hash(block_hash) if block_hash else None
        if isinstance(block, dict): ts_unix = h2i(block.get("timestamp"))
        if ts_unix == 0:
            bn = row.get("blockNumber")
            if bn:
                tag = hex(int(bn)) if str(bn).isdigit() else bn
                block2 = get_block_by_number(tag)
                if isinstance(block2, dict): ts_unix = h2i(block2.get("timestamp"))
    ts_iso = datetime.fromtimestamp(ts_unix, tz=AT_TZ).strftime("%Y-%m-%d %H:%M:%S %Z") if ts_unix else ""

    gas_used = h2i(rcpt.get("gasUsed"))
    eff_price = h2i(rcpt.get("effectiveGasPrice")) or int(row.get("gasPrice") or "0")
    total_gas_eth = wei_str_eth(gas_used * eff_price) if (gas_used and eff_price) else fmt_amount(0, 18, 5, 8)

    if is_failed:
        log_fail(idx_failed, txh, total_gas_eth)
        return {
            "tx_hash": txh,
            "tx_timestamp": ts_iso,
            "block_time": ts_unix,
            "type": "failed",
            "from_address": from_addr,
            "to_address": to_addr,
            "amount_sent": "",
            "amount_received": "",
            "total_gas_eth": total_gas_eth,
            "nft_transfere": "",
            "_failed": True,
        }

    meta_hint = fetch_tokentx_metadata(txh)  # ft hints only

    erc20_eth, nft_moves = compute_wallet_deltas(row, rcpt, WALLET_ADDRESS, meta_hint)

    logs = rcpt.get("logs", []) or []
    action_type = classify_from_topics(logs) or classify_from_balances(erc20_eth)

    amount_sent, amount_received = build_ft_strings(erc20_eth, meta_hint)
    nft_field = build_nft_field(nft_moves, meta_hint)

    log_tx(idx_success, txh, action_type, amount_sent, amount_received, total_gas_eth)

    return {
        "tx_hash": txh,
        "tx_timestamp": ts_iso,
        "block_time": ts_unix,
        "type": action_type,
        "from_address": from_addr,
        "to_address": to_addr,
        "amount_sent": amount_sent,
        "amount_received": amount_received,
        "total_gas_eth": total_gas_eth,
        "nft_transfere": nft_field,  # keep original column name
        "_failed": False,
    }

# --------------------------- main ---------------------------
def main():
    if not os.path.exists(RAW_CSV):
        print(f"ERROR: {RAW_CSV} not found."); return

    already = set()
    existing: dict[str, dict] = {}
    if os.path.exists(OUT_CSV_DECODED) and os.stat(OUT_CSV_DECODED).st_size > 0:
        with open(OUT_CSV_DECODED, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                txh = row.get("tx_hash")
                if not txh: continue
                try: row["block_time"] = int(row.get("block_time") or 0)
                except Exception: row["block_time"] = 0
                existing[txh] = row
                already.add(txh)

    log_info(f"Loaded {len(existing)} previously decoded rows.")

    with open(RAW_CSV, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log_info(f"Loaded {len(rows)} raw rows; {len(already)} already decoded.")

    pending = []
    for row in rows:
        txh = row.get("hash") or row.get("tx_hash")
        if txh and txh not in already: pending.append(row)

    total = len(pending)
    if total == 0:
        log_info("Nothing new to decode."); return

    log_info(f"Decoding {total} transaction(s)...")
    print_progress(0, total)

    new_rows: list[dict] = []
    succ_cnt = fail_cnt = 0

    for i, row in enumerate(pending, 1):
        txh = row.get("hash") or row.get("tx_hash")
        try:
            decoded = decode_one_from_row(row, already, len(new_rows) + 1, len(new_rows) + 1)
            if decoded:
                already.add(txh)
                if decoded.pop("_failed", False): fail_cnt += 1
                else: succ_cnt += 1
                new_rows.append(decoded)
        except Exception as e:
            log_info(f"[err] {txh}: {e}")
            time.sleep(0.3)
        print_progress(i, total)

    merged = dict(existing)
    for r in new_rows: merged[r["tx_hash"]] = r

    out_rows = list(merged.values())
    for r in out_rows:
        try: r["block_time"] = int(r.get("block_time") if r.get("block_time") not in (None, "") else 0)
        except Exception: r["block_time"] = 0
    out_rows.sort(key=lambda r: (r["block_time"], r.get("tx_hash") or ""), reverse=True)

    cols = ["tx_hash","tx_timestamp","block_time","type",
            "from_address","to_address",
            "amount_sent","amount_received","total_gas_eth","nft_transfere"]

    with open(OUT_CSV_DECODED, "w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=cols)
        w.writeheader()
        for r in out_rows:
            w.writerow({
                "tx_hash": r.get("tx_hash", ""),
                "tx_timestamp": r.get("tx_timestamp", ""),
                "block_time": int(r.get("block_time") or 0),
                "type": r.get("type", "unknown"),
                "from_address": r.get("from_address", ""),
                "to_address": r.get("to_address", ""),
                "amount_sent": r.get("amount_sent", ""),
                "amount_received": r.get("amount_received", ""),
                "total_gas_eth": r.get("total_gas_eth", fmt_amount(0, 18, 5, 8)),
                "nft_transfere": r.get("nft_transfere", ""),
            })

    log_info(f"Finished. New: {succ_cnt} success, {fail_cnt} failed. Total written: {len(out_rows)} rows.")
    log_info(f"Output: {OUT_CSV_DECODED}")

if __name__ == "__main__":
    main()
