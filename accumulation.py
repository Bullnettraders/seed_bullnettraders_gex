"""
accumulation.py — Erkennt institutionelle Akkumulation aus historischen Prints
Speichert täglich Prints in JSON, vergleicht über 7 Tage Rolling Window.
"""

import json
import os
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv('DATA_DIR', '/app/data')
ACCUM_FILE = os.path.join(DATA_DIR, 'accumulation.json')

# Cluster-Toleranz: Prices innerhalb ±0.3% gelten als gleiche Zone
CLUSTER_PCT = 0.003
# Minimum Tage mit Aktivität für Signal
MIN_DAYS = 2
# Minimum Gesamtvolumen für Signal
MIN_VOLUME = 100_000


def _ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _load():
    _ensure_dir()
    try:
        with open(ACCUM_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def _save(data):
    _ensure_dir()
    with open(ACCUM_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def save_daily_prints(ticker: str, prints_data: dict):
    """
    Speichert die heutigen Prints für ticker.
    prints_data = {'prints': [{'price': float, 'shares': int, 'side': str}, ...]}
    """
    if not prints_data or not prints_data.get('prints'):
        return

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    data = _load()

    if ticker not in data:
        data[ticker] = {}

    # Speichere top 20 Trades des Tages
    trades = [
        {'price': p['price'], 'shares': p.get('shares', 0), 'side': p.get('side', '?')}
        for p in prints_data['prints']
    ]
    data[ticker][today] = trades

    # Rolling Window: nur letzte 7 Tage behalten
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d')
    data[ticker] = {d: v for d, v in data[ticker].items() if d >= cutoff}

    _save(data)
    logger.info(f"Accumulation: saved {len(trades)} prints for {ticker} on {today}")


def detect_accumulation(ticker: str, lookback_days: int = 7) -> list:
    """
    Analysiert historische Prints und findet Akkumulations-Zonen.
    Returns: Liste von Zonen sortiert nach Stärke (stärkste zuerst)
    """
    data = _load()
    if ticker not in data or not data[ticker]:
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    relevant = {d: v for d, v in data[ticker].items() if d >= cutoff}

    if not relevant:
        return []

    # Alle Trades sammeln mit Datum
    all_trades = []
    for date, trades in relevant.items():
        for t in trades:
            all_trades.append({'date': date, **t})

    if not all_trades:
        return []

    # Clustering: Gruppiere ähnliche Preise (±CLUSTER_PCT)
    clusters = []
    for trade in sorted(all_trades, key=lambda x: x['price']):
        placed = False
        for cluster in clusters:
            ref = cluster['ref_price']
            if abs(trade['price'] - ref) / ref <= CLUSTER_PCT:
                cluster['trades'].append(trade)
                # Update ref_price als gewichteter Durchschnitt
                cluster['ref_price'] = (
                    sum(t['price'] * t['shares'] for t in cluster['trades']) /
                    sum(t['shares'] for t in cluster['trades'])
                )
                placed = True
                break
        if not placed:
            clusters.append({'ref_price': trade['price'], 'trades': [trade]})

    # Auswerten: nur Cluster mit MIN_DAYS und MIN_VOLUME
    signals = []
    for cluster in clusters:
        days_active = len(set(t['date'] for t in cluster['trades']))
        total_vol = sum(t['shares'] for t in cluster['trades'])
        bid_vol = sum(t['shares'] for t in cluster['trades'] if t['side'] == 'Bid')
        ask_vol = sum(t['shares'] for t in cluster['trades'] if t['side'] == 'Ask')
        total_trades = len(cluster['trades'])

        if days_active < MIN_DAYS or total_vol < MIN_VOLUME:
            continue

        bias = 'BULLISH' if bid_vol > ask_vol else 'BEARISH'
        strength = days_active * (total_vol / 100_000)  # Score

        signals.append({
            'price': round(cluster['ref_price'], 2),
            'days': days_active,
            'total_vol': total_vol,
            'bid_vol': bid_vol,
            'ask_vol': ask_vol,
            'total_trades': total_trades,
            'bias': bias,
            'strength': strength,
        })

    # Sortiere nach Stärke
    signals.sort(key=lambda x: x['strength'], reverse=True)
    return signals[:5]  # Top 5 Zonen


def format_accumulation_discord(ticker: str, signals: list, ratio: float) -> str:
    """Formatiert Akkumulations-Report für Discord."""
    cfd_label = 'XAUUSD' if ticker in ('GLD', 'GOLD') else 'NAS100 CFD'

    if not signals:
        return f"```\nBullNet Akkumulation - {ticker}\n{'='*44}\n  Nicht genug historische Daten (min. 2 Tage)\n{'='*44}\n```"

    lines = [
        f"```",
        f"BullNet Akkumulation - {ticker}",
        f"{'='*44}",
        f"  Institutionelle Zonen (7-Tage Rolling)",
        f"",
        f"--- AKKUMULATIONS-ZONEN ---",
        f"",
    ]

    for i, s in enumerate(signals, 1):
        cfd = round(s['price'] * ratio, 0)
        icon = '🟢' if s['bias'] == 'BULLISH' else '🔴'
        lines.append(f"  {icon} Zone {i}: {s['price']} {ticker}  =  {int(cfd)} {cfd_label}")
        lines.append(f"     {s['days']} Tage aktiv | {s['total_vol']:,} Shares total")
        lines.append(f"     Bid: {s['bid_vol']:,} | Ask: {s['ask_vol']:,} | {s['bias']}")
        lines.append(f"")

    lines += [
        f"{'='*44}",
        f"  Lookback: 7 Tage | Cluster: ±0.3%",
        f"```",
    ]

    return '\n'.join(lines)
