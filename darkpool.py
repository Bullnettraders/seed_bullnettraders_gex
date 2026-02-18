"""
BullNet Dark Pool Scanner v2
Direct API calls to ChartExchange — NO Selenium needed.
Falls back to FINRA + options-derived if API fails.
"""

import requests
import logging
import re
import os
from datetime import datetime, timedelta
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  CHARTEXCHANGE SYMBOL IDs (internal)
# ═══════════════════════════════════════════════════════════

# To find new IDs: Open DevTools → Network → XHR → look at datatable POST payload → symbol_
SYMBOL_IDS = {
    'QQQ': '2017015',
    'SPY': '2005153',
    'GLD': '2011862',
    'IWM': '2005154',
    'AAPL': '2000335',
    'MSFT': '2000653',
    'AMZN': '2000349',
    'NVDA': '2000658',
    'TSLA': '2000783',
    'META': '2000423',
    'AMD': '2000348',
    'GOOGL': '2000458',
}

# Exchange prefixes for ChartExchange URLs
EXCHANGE_MAP = {
    'QQQ': 'nasdaq', 'AAPL': 'nasdaq', 'MSFT': 'nasdaq', 'AMZN': 'nasdaq',
    'GOOGL': 'nasdaq', 'META': 'nasdaq', 'NVDA': 'nasdaq', 'TSLA': 'nasdaq',
    'AMD': 'nasdaq', 'NFLX': 'nasdaq', 'INTC': 'nasdaq',
    'GLD': 'nyse', 'SPY': 'nyse', 'IWM': 'nyse',
}


def _discover_symbol_id(ticker):
    """
    Auto-discover ChartExchange symbol ID by fetching the page source.
    The ID is embedded in the JavaScript on the page.
    """
    exchange = EXCHANGE_MAP.get(ticker.upper(), 'nyse')
    url = f"https://chartexchange.com/symbol/{exchange}-{ticker.lower()}/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Discovery: page returned {resp.status_code} for {ticker}")
            return None
        
        # Look for symbol ID patterns in the page source
        # Common patterns: symbol_id = "2017015", "symbolId":"2017015", data-symbol="2017015"
        patterns = [
            r'symbol[_\-]?[iI]d["\s:=]+["\']?(\d{5,10})',
            r'"symbol_":\s*"(\d{5,10})"',
            r"symbol_.*?['\"](\d{5,10})['\"]",
            r'data-symbol[_-]?id[=:]["\'](\d{5,10})',
            r'/(\d{7})\b',  # 7-digit IDs in URLs
        ]
        
        for pattern in patterns:
            m = re.search(pattern, resp.text)
            if m:
                sid = m.group(1)
                logger.info(f"Discovery: found symbol ID {sid} for {ticker}")
                SYMBOL_IDS[ticker.upper()] = sid
                return sid
        
        logger.warning(f"Discovery: could not find symbol ID for {ticker} in page source")
    except Exception as e:
        logger.warning(f"Discovery failed for {ticker}: {e}")
    
    return None


# ═══════════════════════════════════════════════════════════
#  CHARTEXCHANGE API — Direct POST (no Selenium!)
# ═══════════════════════════════════════════════════════════

def fetch_chartexchange_api(ticker="QQQ", max_results=15):
    """
    Fetch dark pool levels directly via ChartExchange's DataTable API.
    No Selenium, no Chrome — just a POST request.
    Returns list of {'price': float, 'volume': int, 'trades': int}.
    """
    symbol_id = SYMBOL_IDS.get(ticker.upper())
    if not symbol_id:
        # Try auto-discovery
        symbol_id = _discover_symbol_id(ticker)
        if not symbol_id:
            logger.warning(f"No ChartExchange symbol ID for {ticker}. Known: {list(SYMBOL_IDS.keys())}")
            return []

    exchange = EXCHANGE_MAP.get(ticker.upper(), 'nyse')

    # Yesterday's date (DP data is T+1)
    today = datetime.now()
    for days_back in range(1, 5):
        date = today - timedelta(days=days_back)
        if date.weekday() < 5:  # Skip weekends
            date_str = date.strftime('%Y-%m-%d')
            break
    else:
        date_str = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    url = "https://chartexchange.com/xhr/dark-pool-levels/datatable"

    payload = {
        "draw": 1,
        "columns": [
            {"data": "level", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "trades", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "notional", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "volume", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ],
        "order": [{"column": 3, "dir": "desc"}],  # Sort by volume descending
        "start": 0,
        "length": max_results,
        "search": {"value": "", "regex": False},
        "symbol_": symbol_id,
        "date_": date_str,
        "decimals_": 2,
        "most_recent_": False,
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Referer': f'https://chartexchange.com/symbol/{exchange}-{ticker.lower()}/exchange-volume/',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://chartexchange.com',
    }

    levels = []

    try:
        logger.info(f"ChartExchange API: POST {url} | symbol={symbol_id} date={date_str}")
        resp = requests.post(url, json=payload, headers=headers, timeout=15)

        if resp.status_code != 200:
            logger.warning(f"ChartExchange API returned {resp.status_code}")
            # Try with most_recent_ = True as fallback
            payload["most_recent_"] = True
            payload.pop("date_", None)
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"ChartExchange API fallback also returned {resp.status_code}")
                return []

        data = resp.json()
        records = data.get('data', [])
        logger.info(f"ChartExchange API: got {len(records)} records")

        for rec in records:
            try:
                # DataTables returns rendered HTML — extract numbers
                price = _extract_number(rec.get('level', ''))
                volume = _extract_number(rec.get('volume', ''))
                trades = _extract_number(rec.get('trades', ''))

                if price and price > 50 and volume:
                    levels.append({
                        'price': price,
                        'volume': int(volume),
                        'trades': int(trades) if trades else 0,
                    })
                    logger.debug(f"  Level: {price} | Vol: {volume:,.0f} | Trades: {trades}")
            except Exception as e:
                logger.debug(f"  Skip record: {e}")
                continue

        logger.info(f"ChartExchange API: parsed {len(levels)} valid levels")

    except Exception as e:
        logger.error(f"ChartExchange API error: {e}")

    return levels


def _extract_number(val):
    """Extract a number from a DataTable cell (might be HTML or plain text)."""
    if val is None:
        return None
    # Convert to string and strip HTML tags
    s = str(val)
    s = re.sub(r'<[^>]+>', '', s)
    s = s.replace(',', '').replace('$', '').strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        # Try extracting first number
        m = re.search(r'[\d.]+', s)
        if m:
            return float(m.group())
    return None


# ═══════════════════════════════════════════════════════════
#  CHARTEXCHANGE — Dark Pool Prints (individual trades)
# ═══════════════════════════════════════════════════════════

def fetch_chartexchange_prints(ticker="QQQ", max_results=50):
    """
    Fetch dark pool prints (individual large trades) via ChartExchange API.
    Returns list of {'price': float, 'shares': int, 'dollar_volume': float}.
    """
    symbol_id = SYMBOL_IDS.get(ticker.upper())
    if not symbol_id:
        symbol_id = _discover_symbol_id(ticker)
        if not symbol_id:
            return []
    exchange = EXCHANGE_MAP.get(ticker.upper(), 'nyse')

    today = datetime.now()
    for days_back in range(1, 5):
        date = today - timedelta(days=days_back)
        if date.weekday() < 5:
            date_str = date.strftime('%Y-%m-%d')
            break
    else:
        date_str = (today - timedelta(days=1)).strftime('%Y-%m-%d')

    url = "https://chartexchange.com/xhr/dark-pool-prints/datatable"

    payload = {
        "draw": 1,
        "columns": [
            {"data": "time", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "price", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "size", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "notional", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ],
        "order": [{"column": 3, "dir": "desc"}],  # Sort by notional value
        "start": 0,
        "length": max_results,
        "search": {"value": "", "regex": False},
        "symbol_": symbol_id,
        "date_": date_str,
        "decimals_": 2,
        "most_recent_": False,
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Referer': f'https://chartexchange.com/symbol/{exchange}-{ticker.lower()}/exchange-volume/',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://chartexchange.com',
    }

    prints = []

    try:
        logger.info(f"ChartExchange Prints API: POST {url}")
        resp = requests.post(url, json=payload, headers=headers, timeout=15)

        if resp.status_code != 200:
            logger.warning(f"ChartExchange Prints API returned {resp.status_code}")
            return []

        data = resp.json()
        records = data.get('data', [])
        logger.info(f"ChartExchange Prints: got {len(records)} records")

        for rec in records:
            try:
                price = _extract_number(rec.get('price', ''))
                shares = _extract_number(rec.get('size', ''))
                notional = _extract_number(rec.get('notional', ''))

                if price and price > 50 and shares:
                    prints.append({
                        'price': price,
                        'shares': int(shares),
                        'dollar_volume': notional or (price * shares),
                    })
            except:
                continue

    except Exception as e:
        logger.error(f"ChartExchange Prints API error: {e}")

    return prints


# ═══════════════════════════════════════════════════════════
#  FINRA SHORT VOLUME
# ═══════════════════════════════════════════════════════════

def fetch_finra_volume(ticker="QQQ"):
    """Fetch FINRA OTC/ATS short volume data."""
    today = datetime.now()
    for days_back in range(1, 5):
        date = today - timedelta(days=days_back)
        if date.weekday() >= 5:
            continue
        date_str = date.strftime('%Y%m%d')
        url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date_str}.txt"
        headers = {'User-Agent': 'Mozilla/5.0'}

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                continue
            lines = resp.text.strip().split('\n')
            for line in lines:
                parts = line.split('|')
                if len(parts) >= 5 and parts[1].upper() == ticker.upper():
                    short_vol = int(parts[2]) if parts[2].isdigit() else 0
                    total_vol = int(parts[4]) if parts[4].isdigit() else 0
                    short_pct = (short_vol / total_vol * 100) if total_vol > 0 else 0
                    logger.info(f"FINRA {date_str}: {ticker} Short: {short_vol:,} / Total: {total_vol:,} ({short_pct:.1f}%)")
                    return {
                        'date': date.strftime('%Y-%m-%d'),
                        'short_volume': short_vol,
                        'total_volume': total_vol,
                        'short_percent': round(short_pct, 1),
                    }
        except:
            continue
    return None


# ═══════════════════════════════════════════════════════════
#  OPTIONS-DERIVED FALLBACK
# ═══════════════════════════════════════════════════════════

def derive_dp_levels_from_options(spot, gex_df):
    """Fallback: derive DP-style levels from options OI/volume."""
    if gex_df is None or gex_df.empty:
        return []

    levels = []
    df = gex_df.copy()
    df['total_score'] = df['total_volume'] * 2 + df['total_oi']

    near = df[abs(df['strike'] - spot) / spot <= 0.05].copy()
    if near.empty:
        near = df.copy()
    near = near.sort_values('total_score', ascending=False)

    for _, row in near.head(8).iterrows():
        strike = row['strike']
        vol = int(row['total_volume'])
        oi = int(row['total_oi'])

        if strike > spot * 1.005:
            tp = "DP Resistance"
        elif strike < spot * 0.995:
            tp = "DP Support"
        else:
            tp = "High Volume"

        if vol > near['total_volume'].quantile(0.9):
            tp = "Block Trade"

        levels.append({
            'strike': strike, 'type': tp,
            'volume': vol, 'oi': oi,
            'score': float(row['total_score']),
        })

    levels.sort(key=lambda x: x['strike'])
    return levels


# ═══════════════════════════════════════════════════════════
#  MAIN — Combine all sources
# ═══════════════════════════════════════════════════════════

def get_dark_pool_levels(ticker="QQQ", spot=None, gex_df=None):
    """
    Get dark pool levels. Priority:
    1. ChartExchange API (direct POST — no Selenium!)
    2. Options-derived — Fallback aus OI/Volume
    Always includes FINRA short volume.
    """
    result = {
        'ticker': ticker,
        'timestamp': datetime.now().isoformat(),
        'source': None,
        'levels': [],
        'finra': None,
    }

    # 1. Try ChartExchange direct API
    try:
        levels_data = fetch_chartexchange_api(ticker)
    except Exception as e:
        logger.warning(f"ChartExchange API failed: {e}")
        levels_data = []

    if levels_data and len(levels_data) >= 3:
        result['source'] = 'chartexchange'

        # Filter: only keep levels within ±20% of spot (sanity check)
        if spot and spot > 0:
            levels_data = [l for l in levels_data if abs(l['price'] - spot) / spot < 0.20]
            logger.info(f"After spot filter (±20% of {spot}): {len(levels_data)} levels remain")

        for lvl in sorted(levels_data, key=lambda x: x['volume'], reverse=True)[:8]:
            strike = lvl['price']
            vol = lvl['volume']
            trades = lvl.get('trades', 0)

            if spot:
                if strike > spot * 1.005:
                    tp = "DP Resistance"
                elif strike < spot * 0.995:
                    tp = "DP Support"
                else:
                    tp = "High Volume"
                if vol > 500000:
                    tp = "Block Trade"
            else:
                tp = "High Volume"

            result['levels'].append({
                'strike': strike, 'type': tp,
                'volume': vol, 'trades': trades,
                'dollar_volume': strike * vol,
            })

        result['levels'].sort(key=lambda x: x['strike'])

    # 2. Fallback: Options-derived
    if not result['levels']:
        logger.info("ChartExchange API returned no data, trying options-derived fallback...")

        if (gex_df is None or (hasattr(gex_df, 'empty') and gex_df.empty)) and spot:
            try:
                from gex_calculator import fetch_cboe_options, parse_options, calculate_gex
                cboe_spot, options = fetch_cboe_options(ticker)
                if not spot:
                    spot = cboe_spot
                df = parse_options(cboe_spot or spot, options)
                if not df.empty:
                    gex_df = calculate_gex(cboe_spot or spot, df)
            except Exception as e:
                logger.warning(f"CBOE fallback failed: {e}")

        if spot and gex_df is not None and not (hasattr(gex_df, 'empty') and gex_df.empty):
            result['source'] = 'options-derived'
            result['levels'] = derive_dp_levels_from_options(spot, gex_df)

    # 3. FINRA short volume (always)
    finra = fetch_finra_volume(ticker)
    if finra:
        result['finra'] = finra

    logger.info(f"Dark Pool: {len(result['levels'])} levels from {result['source']}")
    return result


# ═══════════════════════════════════════════════════════════
#  DISCORD FORMAT
# ═══════════════════════════════════════════════════════════

def format_dp_discord(dp_data, ratio=41.33, ticker="QQQ"):
    """Format dark pool data for Discord."""
    def to_cfd(p):
        return round(p * ratio, 2)

    is_gold = ticker.upper() in ("GLD", "GOLD")
    etf_label = "GLD" if is_gold else "QQQ"
    cfd_label = "XAUUSD" if is_gold else "CFD"
    title = "BullNet Dark Pool - GOLD" if is_gold else f"BullNet Dark Pool - {ticker.upper()}"

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    source = dp_data.get('source', 'N/A')
    levels = dp_data.get('levels', [])
    finra = dp_data.get('finra')

    lines = []
    lines.append(title)
    lines.append("=" * 44)
    lines.append(f"  {now}  |  Source: {source}")
    lines.append("")

    if finra:
        short_pct = finra['short_percent']
        signal = "BEARISH" if short_pct > 55 else "Neutral" if short_pct > 45 else "BULLISH"
        emoji = "🔴" if short_pct > 55 else "⚪" if short_pct > 45 else "🟢"
        lines.append(f"  FINRA Short Volume ({finra['date']}):")
        lines.append(f"  Short: {finra['short_volume']:,} / Total: {finra['total_volume']:,}")
        lines.append(f"  Short %: {short_pct}% = {signal} {emoji}")
        lines.append("")

    if levels:
        lines.append("--- DARK POOL LEVELS ---")
        lines.append("")

        for i, lvl in enumerate(levels[:8], 1):
            strike = lvl['strike']
            tp = lvl['type']
            vol = lvl.get('volume', 0)
            trades = lvl.get('trades', 0)
            vol_str = f"{vol:,}" if vol else "N/A"
            trade_str = f" | {trades:,} Trades" if trades else ""

            icon = "S" if "Support" in tp else "R" if "Resistance" in tp else "HV" if "High" in tp else "BT"
            lines.append(f"  [{icon}] {tp}:")
            lines.append(f"      {strike:.2f} {etf_label}  =  {to_cfd(strike):.2f} {cfd_label}  |  Vol: {vol_str}{trade_str}")
            lines.append("")

        lines.append(f"--- {cfd_label} INPUT ---")
        lines.append("")
        for i, lvl in enumerate(levels[:8], 1):
            lines.append(f"  Zone {i}: {lvl['strike']:.2f} {etf_label} = {to_cfd(lvl['strike']):.2f} {cfd_label}  ({lvl['type']})")
        lines.append("")

    else:
        lines.append("  Keine Dark Pool Daten verfuegbar.")
        lines.append("")

    lines.append("=" * 44)
    lines.append(f"  Ratio: {ratio:.4f} | Daten: Vortag (T+1)")

    return "```\n" + "\n".join(lines) + "\n```"


if __name__ == "__main__":
    dp = get_dark_pool_levels("QQQ", spot=602.0)
    print(format_dp_discord(dp))
