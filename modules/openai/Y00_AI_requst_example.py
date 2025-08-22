# file: functions/Y00_AI_requst_example.py

import sys
from pathlib import Path
from openai import OpenAI

def load_api_key() -> str:
    """Read the OpenAI API key from config/api_key_openai.txt."""
    key_path = Path(__file__).resolve().parents[2] / "config" / "api_key_openai.txt"
    try:
        return key_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        print(f"Error: Could not find {key_path}", file=sys.stderr)
        sys.exit(1)

def ask_ai(prompt: str, model: str = "gpt-4.1-mini") -> str:
    client = OpenAI(api_key=load_api_key())
    resp = client.responses.create(
        model=model,
        input=prompt,
    )
    return resp.output_text

def main() -> int:
    prompt = "tx_hash,tx_timestamp,block_time,type,from_address,to_address,amount_sent,amount_received,total_gas_eth,nft_transfere 0x2e045559039011ffec1819b8ffe39b600cb8475ad6a1443575df7a216ac5047b,2025-08-19 22:44:25 CEST,1755636265,swap,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x6131B5fae19EA4f9D964eAc0408E4408b66337b5,-113.890185309506 REX,+46.78194 USDC,0.0000155, 0xb3b4855c9c42a9fc77f6d9302161d7feba62cdefd1c26334874ed00e4f623ff0,2025-08-19 22:32:47 CEST,1755635567,add_liquidity,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0xA04A9F0a961f8fcc4a94bCF53e676B236cBb2F58,-0.07867705 WBTC; -1.834328388032 ETH,,0.00001855,+Etherex V3 Non Fungible Position#70247 0x7223956339c8f3a4e6e2cd784c016dab436f0a89f693091eec64288de9d18108,2025-08-19 22:32:29 CEST,1755635549,swap,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x2d8879046f1559E53eb052E949e9544bCB72f414,-2.151364983518 ETH,+0.07866745 WBTC,0.00006302, 0xe894f77879f5dc7ed0f3b86e68b254c6ced9024641b520f69ab993b34b7ce9e2,2025-08-19 22:31:31 CEST,1755635491,remove_liquidity,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0xA04A9F0a961f8fcc4a94bCF53e676B236cBb2F58,,+24.29202273228 REX; +3.98634744759 ETH,0.00001962,-Etherex V3 Non Fungible Position#70120 0xc2d6c3ca311bd49a414e2fd4424f091b18b61d5544bd8f9ff9c65fe122c430d1,2025-08-19 22:06:52 CEST,1755634012,add_liquidity,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0xA04A9F0a961f8fcc4a94bCF53e676B236cBb2F58,-0.09499173 WBTC; -1.394056779655 ETH,,0.00001874,+Etherex V3 Non Fungible Position#70120 0x915264593c4e8ff1a9f1c1664bd48049225af2223ca574e61c809ef446a3cf3a,2025-08-19 22:06:34 CEST,1755633994,swap,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x2d8879046f1559E53eb052E949e9544bCB72f414,-0.05110175 WBTC,+1.393967396109 ETH,0.00001247, 0xb7718af77816ae5f61b2eb7a7d5ba9314dcec49eaaa08a73aaa5e9dadf341193,2025-08-19 21:55:39 CEST,1755633339,remove_liquidity,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0xA04A9F0a961f8fcc4a94bCF53e676B236cBb2F58,,+89.598162577226 REX; +0.14610308 WBTC,0.00002065,-Etherex V3 Non Fungible Position#69841 0x21db81fcdd4620a61680eb54183bad77a815051aefeb32ba63bc0a267e5c52f7,2025-08-19 21:23:30 CEST,1755631410,swap,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x6131B5fae19EA4f9D964eAc0408E4408b66337b5,-1650.624248411363 REX,+674.785132 USDC,0.00002614, 0xfaa5d0088a7151b70f7f9771f6415b85f259aed478df9059a5d71d498500a6b8,2025-08-19 21:23:08 CEST,1755631388,failed,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x6131B5fae19EA4f9D964eAc0408E4408b66337b5,,,0.00002699, 0xaaed68d65d60e9e1cfd267267c96bee0073a6507c9c601f0c78caeb7c64f0964,2025-08-19 21:08:42 CEST,1755630522,add_liquidity,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0xA04A9F0a961f8fcc4a94bCF53e676B236cBb2F58,-0.08963153 WBTC; -1.541887630984 ETH,,0.00001855,+Etherex V3 Non Fungible Position#69841 0xc42128b7e6ffb8384b2cd9ab5a1eb05d032288114d014dc1b53f73825fadb1f8,2025-08-19 21:08:18 CEST,1755630498,swap,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x2d8879046f1559E53eb052E949e9544bCB72f414,-0.00876791 WBTC,+0.239650572214 ETH,0.00001247, 0x39cfa7b2ef93547fa59f95a67d52847de3fd181a335254841898e6e354162d52,2025-08-19 21:07:54 CEST,1755630474,failed,0x42Fd11266E2b05E7A86576774049f6fAab6582E9,0x6131B5fae19EA4f9D964eAc0408E4408b66337b5,,,0.00002166, whats the pnl of this transaction? Please provide a detailed analysis of the profit and loss (PnL) for each transaction, including the amounts sent and received, gas fees, and any relevant market conditions at the time of the transactions. Also, consider the impact of token price fluctuations on the PnL calculation."
    
    try:
        answer = ask_ai(prompt, model="gpt-4o")
        print(answer)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

