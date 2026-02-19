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
REPO_NAME = 'pine_seeds'


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

    # Use midnight UTC timestamp — TradingView request.seed() matches on day boundaries
    now = datetime.now(timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    unix_ts = int(midnight.timestamp())

    # Build CSV — TradingView needs: time, open, high, low, close, volume
    # We keep a rolling history (last 30 days) so TradingView can plot
    csv_header = "time,open,high,low,close,volume"
    new_row = f"{unix_ts},{gf},{cw},{pw},{hvl},{regime_val}"

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

            # Keep header + existing data rows (max 30 days)
            if len(lines) > 1:
                existing_rows = lines[1:]  # skip header
                # Keep last 29 rows + new one = 30 total
                existing_rows = existing_rows[-29:]

        # Build final CSV — replace today's row if exists, otherwise append
        today_ts_str = str(unix_ts)
        filtered_rows = [r for r in existing_rows if not r.startswith(today_ts_str + ",")]
        all_rows = filtered_rows[-29:] + [new_row]
        csv_content = csv_header + "\n" + "\n".join(all_rows) + "\n"

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


def push_dp_to_github(ticker="QQQ", dp_data=None):
    """
    Push Dark Pool levels as CSV to GitHub pine_seeds repo.
    Creates/updates: data/{ticker}_dp.csv
    
    Encodes top 4 DP levels into OHLCV:
        open  = DP Zone 1 (ETF price)
        high  = DP Zone 2
        low   = DP Zone 3
        close = DP Zone 4
        volume = number of levels available
    """
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        logger.warning("GitHub token/username not set — skipping DP push")
        return False

    if not dp_data or not dp_data.get('levels'):
        logger.warning("No DP levels to push")
        return False

    levels = dp_data['levels']
    # Take top 4 by volume
    top4 = sorted(levels, key=lambda x: x.get('volume', 0), reverse=True)[:4]
    # Sort by price for Zone ordering
    top4 = sorted(top4, key=lambda x: x['strike'])

    dp1 = top4[0]['strike'] if len(top4) > 0 else 0
    dp2 = top4[1]['strike'] if len(top4) > 1 else 0
    dp3 = top4[2]['strike'] if len(top4) > 2 else 0
    dp4 = top4[3]['strike'] if len(top4) > 3 else 0

    now = datetime.now(timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    unix_ts = int(midnight.timestamp())

    csv_header = "time,open,high,low,close,volume"
    new_row = f"{unix_ts},{dp1},{dp2},{dp3},{dp4},{len(levels)}"

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
            if len(lines) > 1:
                existing_rows = lines[1:][-29:]

        today_ts_str = str(unix_ts)
        filtered_rows = [r for r in existing_rows if not r.startswith(today_ts_str + ",")]
        all_rows = filtered_rows[-29:] + [new_row]
        csv_content = csv_header + "\n" + "\n".join(all_rows) + "\n"
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
