# execute_fetch.py
# Version: 1.0.0

import subprocess
import sys

def run_script(script_path):
    """Run a Python script using subprocess."""
    try:
        print(f"\n[Executing] {script_path} ...")
        subprocess.run([sys.executable, script_path], check=True)
        print(f"[Completed] {script_path}\n")
    except subprocess.CalledProcessError as e:
        print(f"[Error] Script {script_path} failed with return code {e.returncode}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    scripts = [
        r"C:\TrueBlocks\modules\fetch\A00_fetch_txs.py",
        r"C:\TrueBlocks\modules\fetch\A01_decode_txs.py",
        r"C:\TrueBlocks\modules\fetch\A03_onchain_price_REX_USDC.py",
        r"C:\TrueBlocks\modules\fetch\A04_fiat_price_EUR_USD.py",
    ]

    for script in scripts:
        run_script(script)

    print("\n✅ All scripts executed successfully.")

# Version: 1.0.0
