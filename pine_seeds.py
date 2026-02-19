"""
BullNet Pine Seeds — Push GEX levels to GitHub for TradingView auto-import.

TradingView's request.seed() reads CSV files from a public GitHub repo
named 'pine_seeds' under the user's account.

File format required by TradingView:
- CSV with header: time,open,high,low,close,volume (OHLCV format)
- We encode our levels into these fields:
    time   = Unix timestamp (date of the data)
    open   = Gamma Flip
    high   = Call Wall  
    low    = Put Wall
    close  = HVL
    volume = Gamma Regime (1 = Positiv, -1 = Negativ, 0 = N/A)
"""

import os
import json
import base64
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
GITHUB_USERNAME = os.getenv('GITHUB_USERNAME', '')
REPO_NAME = os.getenv('PINE_SEEDS_REPO', 'seed_bullnettraders_gex')


def push_gex_to_github(ticker="QQQ", levels=None, spot=0):
    """
    Push GEX levels as CSV to GitHub pine_seeds repo.
    Creates/updates the file: {REPO}/data/{ticker}_gex.csv
    """
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        logger.warning("GitHub token or username not set — skipping pine_seeds push")
        return False

    if not levels:
        logger.warning("No levels to push")
        return False

    gf = levels.get('gamma_flip', 0)
    cw = levels.get('call_wall', 0)
    pw = levels.get('put_wall', 0)
    hvl = levels.get('hvl', 0)
    regime = levels.get('gamma_regime', 'N/A')
    source = levels.get('source', 'unknown')

    # Encode regime as number for OHLCV
    regime_val = 1 if regime == "Positiv" else -1 if regime == "Negativ" else 0

    # Use YYYYMMDDT format — TradingView requirement for pine_seeds
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%dT")

    # Build CSV — NO HEADER, TradingView pine_seeds format
    # Format: YYYYMMDDT,open,high,low,close,volume
    new_row = f"{date_str},{gf},{cw},{pw},{hvl},{regime_val}"

    # Determine file path in repo
    filename = f"{ticker.upper()}_gex"
    filepath = f"data/{filename}.csv"

    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{REPO_NAME}/contents/{filepath}"
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'BullNet-Bot',
    }

    try:
        # Check if file exists (get current SHA for update)
        resp = requests.get(api_url, headers=headers, timeout=15)
        existing_sha = None
        existing_rows = []

        if resp.status_code == 200:
            file_data = resp.json()
            existing_sha = file_data['sha']

            # Decode existing content
            content = base64.b64decode(file_data['content']).decode('utf-8')
            lines = content.strip().split('\n')

            # Filter out: header rows, empty lines, today's row (will be replaced)
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('time') or line.startswith('time,'):
                    continue  # skip old header
                if line.startswith(date_str):
                    continue  # skip today's old row
                # Also skip old unix timestamp format rows
                if line[0].isdigit() and 'T' not in line.split(',')[0]:
                    continue  # skip old format rows
                existing_rows.append(line)

            # Keep last 29 rows + new one = 30 total
            existing_rows = existing_rows[-29:]

        # Build final CSV — NO HEADER
        all_rows = existing_rows + [new_row]
        csv_content = "\n".join(all_rows) + "\n"

        # Encode to base64
        content_b64 = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')

        # Create or update file
        payload = {
            'message': f'GEX update {ticker} — {now.strftime("%Y-%m-%d %H:%M")} UTC | GF:{gf} CW:{cw} PW:{pw} | {source}',
            'content': content_b64,
        }
        if existing_sha:
            payload['sha'] = existing_sha

        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()

        logger.info(f"Pine Seeds: pushed {ticker} GEX to GitHub — GF:{gf} CW:{cw} PW:{pw} HVL:{hvl} Regime:{regime}")
        return True

    except Exception as e:
        logger.error(f"Pine Seeds push failed: {e}")
        return False


def push_all_levels(nasdaq_levels=None, nasdaq_spot=0, gold_levels=None, gold_spot=0):
    """Push both Nasdaq and Gold levels."""
    success = True

    if nasdaq_levels:
        if not push_gex_to_github("QQQ", nasdaq_levels, nasdaq_spot):
            success = False

    if gold_levels:
        if not push_gex_to_github("GLD", gold_levels, gold_spot):
            success = False

    return success


def push_dp_to_github(ticker="QQQ", dp_data=None, dp_zones=None):
    """
    Push Dark Pool zones as CSV to GitHub pine_seeds repo.
    Creates/updates: data/{ticker}_dp.csv
    
    Encodes top 4 DP zones (by volume, sorted by price) into OHLCV:
        open  = DP Zone 1
        high  = DP Zone 2
        low   = DP Zone 3
        close = DP Zone 4
        volume = 1 (placeholder)
    """
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        logger.warning("GitHub token/username not set — skipping DP push")
        return False

    # Use dp_zones if provided, otherwise try to extract from dp_data
    if dp_zones:
        dp1 = dp_zones.get('dp1', 0)
        dp2 = dp_zones.get('dp2', 0)
        dp3 = dp_zones.get('dp3', 0)
        dp4 = dp_zones.get('dp4', 0)
    elif dp_data and dp_data.get('levels'):
        levels = dp_data['levels']
        top4 = sorted(levels, key=lambda x: x.get('volume', 0), reverse=True)[:4]
        top4 = sorted(top4, key=lambda x: x['strike'])
        dp1 = top4[0]['strike'] if len(top4) > 0 else 0
        dp2 = top4[1]['strike'] if len(top4) > 1 else 0
        dp3 = top4[2]['strike'] if len(top4) > 2 else 0
        dp4 = top4[3]['strike'] if len(top4) > 3 else 0
    else:
        logger.warning("No DP data to push")
        return False

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%dT")

    # NO HEADER — TradingView pine_seeds format: YYYYMMDDT,open,high,low,close,volume
    # open=DP1, high=DP2, low=DP3, close=DP4
    new_row = f"{date_str},{dp1},{dp2},{dp3},{dp4},1"

    filename = f"{ticker.upper()}_dp"
    filepath = f"data/{filename}.csv"

    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{REPO_NAME}/contents/{filepath}"
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'BullNet-Bot',
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=15)
        existing_sha = None
        existing_rows = []

        if resp.status_code == 200:
            file_data = resp.json()
            existing_sha = file_data['sha']
            content = base64.b64decode(file_data['content']).decode('utf-8')
            lines = content.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line or line.startswith('time'):
                    continue
                if line.startswith(date_str):
                    continue
                if line[0].isdigit() and 'T' not in line.split(',')[0]:
                    continue
                existing_rows.append(line)
            existing_rows = existing_rows[-29:]

        all_rows = existing_rows + [new_row]
        csv_content = "\n".join(all_rows) + "\n"
        content_b64 = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')

        payload = {
            'message': f'DP update {ticker} — {now.strftime("%Y-%m-%d %H:%M")} UTC | {dp1}/{dp2}/{dp3}/{dp4} | {dp_data.get("source", "?")}',
            'content': content_b64,
        }
        if existing_sha:
            payload['sha'] = existing_sha

        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()

        logger.info(f"Pine Seeds: pushed {ticker} DP to GitHub — Z1:{dp1} Z2:{dp2} Z3:{dp3} Z4:{dp4}")
        return True

    except Exception as e:
        logger.error(f"Pine Seeds DP push failed: {e}")
        return False


def ensure_symbol_info():
    """
    Ensure symbol_info JSON exists in the repo.
    TradingView requires this for pine_seeds to work.
    """
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return False

    symbol_info = {
        "QQQ_gex": {
            "symbol": "QQQ_gex",
            "description": "BullNet QQQ GEX Levels",
            "pricescale": 100
        },
        "GLD_gex": {
            "symbol": "GLD_gex",
            "description": "BullNet GLD GEX Levels",
            "pricescale": 100
        },
        "QQQ_dp": {
            "symbol": "QQQ_dp",
            "description": "BullNet QQQ Dark Pool Zones",
            "pricescale": 100
        },
        "GLD_dp": {
            "symbol": "GLD_dp",
            "description": "BullNet GLD Dark Pool Zones",
            "pricescale": 100
        }
    }

    filepath = f"symbol_info/{REPO_NAME}.json"
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{REPO_NAME}/contents/{filepath}"
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'BullNet-Bot',
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            logger.info("symbol_info already exists")
            return True

        import json as json_mod
        content = json_mod.dumps(symbol_info, indent=4)
        content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')

        payload = {
            'message': 'Add symbol_info for TradingView pine_seeds',
            'content': content_b64,
        }
        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info("symbol_info created successfully")
        return True
    except Exception as e:
        logger.error(f"symbol_info push failed: {e}")
        return False


if __name__ == "__main__":
    # Test push
    test_levels = {
        'gamma_flip': 622.04,
        'call_wall': 610.0,
        'put_wall': 600.0,
        'hvl': 600.0,
        'gamma_regime': 'Negativ',
        'source': 'test',
    }
    push_gex_to_github("QQQ", test_levels, spot=601.3)
