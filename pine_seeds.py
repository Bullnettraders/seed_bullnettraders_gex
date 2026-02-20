"""
BullNet Pine Seeds — Push GEX levels to GitHub for TradingView auto-import.
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
    regime_val = 1 if regime == "Positiv" else -1 if regime == "Negativ" else 0

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%dT")
    new_row = f"{date_str},{gf},{cw},{pw},{hvl},{regime_val}"

    filepath = f"data/{ticker.upper()}_gex.csv"
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
            for line in content.strip().split('\n'):
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
            'message': f'GEX update {ticker} — {now.strftime("%Y-%m-%d %H:%M")} UTC | GF:{gf} CW:{cw} PW:{pw} | {source}',
            'content': content_b64,
        }
        if existing_sha:
            payload['sha'] = existing_sha

        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info(f"Pine Seeds: pushed {ticker} GEX — GF:{gf} CW:{cw} PW:{pw} HVL:{hvl}")
        return True
    except Exception as e:
        logger.error(f"Pine Seeds GEX push failed: {e}")
        return False


def push_dp_to_github(ticker="QQQ", dp_data=None, dp_zones=None):
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        logger.warning("GitHub token/username not set — skipping DP push")
        return False

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
    new_row = f"{date_str},{dp1},{dp2},{dp3},{dp4},1"

    filepath = f"data/{ticker.upper()}_dp.csv"
    api_url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{REPO_NAME}/contents/{filepath}"
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'BullNet-Bot',
    }

    # ✅ FIX: source aus dp_data nur wenn dp_data nicht None
    source_label = dp_data.get('source', '?') if dp_data else 'zones'

    try:
        resp = requests.get(api_url, headers=headers, timeout=15)
        existing_sha = None
        existing_rows = []

        if resp.status_code == 200:
            file_data = resp.json()
            existing_sha = file_data['sha']
            content = base64.b64decode(file_data['content']).decode('utf-8')
            for line in content.strip().split('\n'):
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
            'message': f'DP update {ticker} — {now.strftime("%Y-%m-%d %H:%M")} UTC | {dp1}/{dp2}/{dp3}/{dp4} | {source_label}',
            'content': content_b64,
        }
        if existing_sha:
            payload['sha'] = existing_sha

        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info(f"Pine Seeds: pushed {ticker} DP — Z1:{dp1} Z2:{dp2} Z3:{dp3} Z4:{dp4}")
        return True
    except Exception as e:
        logger.error(f"Pine Seeds DP push failed: {e}")
        return False


def push_all_levels(nasdaq_levels=None, nasdaq_spot=0, gold_levels=None, gold_spot=0):
    success = True
    if nasdaq_levels:
        if not push_gex_to_github("QQQ", nasdaq_levels, nasdaq_spot):
            success = False
    if gold_levels:
        if not push_gex_to_github("GLD", gold_levels, gold_spot):
            success = False
    return success


def push_bt_to_github(ticker="QQQ", prints_data=None):
    """
    Push top 3 Block Trade prices to GitHub pine_seeds repo.
    Creates/updates: data/{ticker}_BT.csv
    CSV: open=BT1, high=BT2, low=BT3 (top trades by volume, sorted by price)
    """
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return False
    if not prints_data or not prints_data.get('prints'):
        logger.warning(f"No prints data to push for {ticker}")
        return False

    prints = prints_data.get('prints', [])
    # Sort by shares volume desc, take top 3, then sort by price
    top3 = sorted(prints, key=lambda x: x.get('shares', 0), reverse=True)[:3]
    top3 = sorted(top3, key=lambda x: x.get('price', 0))

    bt1 = round(top3[0]['price'], 2) if len(top3) > 0 else 0
    bt2 = round(top3[1]['price'], 2) if len(top3) > 1 else 0
    bt3 = round(top3[2]['price'], 2) if len(top3) > 2 else 0

    if bt1 == 0:
        logger.warning(f"No valid BT prices for {ticker}")
        return False

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%dT")
    new_row = f"{date_str},{bt1},{bt2},{bt3},0,1"

    filepath = f"data/{ticker.upper()}_BT.csv"
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
            content_raw = base64.b64decode(file_data['content']).decode('utf-8')
            for line in content_raw.strip().split('\n'):
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
            'message': f'BT update {ticker} — {now.strftime("%Y-%m-%d %H:%M")} UTC | {bt1}/{bt2}/{bt3}',
            'content': content_b64,
        }
        if existing_sha:
            payload['sha'] = existing_sha

        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info(f"Pine Seeds: pushed {ticker} BT — BT1:{bt1} BT2:{bt2} BT3:{bt3}")
        return True
    except Exception as e:
        logger.error(f"Pine Seeds BT push failed: {e}")
        return False


def ensure_symbol_info():
    if not GITHUB_TOKEN or not GITHUB_USERNAME:
        return False

    symbol_info = {
        "QQQ_gex": {"symbol": "QQQ_gex", "description": "BullNet QQQ GEX Levels", "pricescale": 100},
        "GLD_gex": {"symbol": "GLD_gex", "description": "BullNet GLD GEX Levels", "pricescale": 100},
        "QQQ_dp":  {"symbol": "QQQ_dp",  "description": "BullNet QQQ Dark Pool Zones", "pricescale": 100},
        "GLD_dp":  {"symbol": "GLD_dp",  "description": "BullNet GLD Dark Pool Zones", "pricescale": 100},
        "QQQ_BT":  {"symbol": "QQQ_BT",  "description": "BullNet QQQ Block Trades", "pricescale": 100},
        "GLD_BT":  {"symbol": "GLD_BT",  "description": "BullNet GLD Block Trades", "pricescale": 100},
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

        content = json.dumps(symbol_info, indent=4)
        content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        payload = {'message': 'Add symbol_info for TradingView pine_seeds', 'content': content_b64}
        resp = requests.put(api_url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        logger.info("symbol_info created successfully")
        return True
    except Exception as e:
        logger.error(f"symbol_info push failed: {e}")
        return False
