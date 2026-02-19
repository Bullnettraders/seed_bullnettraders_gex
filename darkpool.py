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
    # GLD: auto-discovered (exchange varies: nyse/amex/nysearca)
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

# Alternative exchanges to try if primary fails
EXCHANGE_ALTERNATIVES = {
    'GLD': ['amex', 'nysearca', 'nyse'],
    'SLV': ['amex', 'nysearca', 'nyse'],
}


def _discover_symbol_id(ticker):
    """
    Auto-discover ChartExchange symbol ID by fetching the page source.
    The ID is embedded in the JavaScript on the page.
    Tries multiple exchange prefixes if needed.
    """
    exchanges_to_try = EXCHANGE_ALTERNATIVES.get(ticker.upper(), [])
    primary = EXCHANGE_MAP.get(ticker.upper(), 'nyse')
    if primary not in exchanges_to_try:
        exchanges_to_try = [primary] + exchanges_to_try
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    }
    
    for exchange in exchanges_to_try:
        url = f"https://chartexchange.com/symbol/{exchange}-{ticker.lower()}/"
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                logger.info(f"Discovery: {exchange}-{ticker.lower()} returned {resp.status_code}, trying next...")
                continue
            
            # Look for symbol ID patterns in the page source
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
                    logger.info(f"Discovery: found symbol ID {sid} for {ticker} via {exchange}")
                    SYMBOL_IDS[ticker.upper()] = sid
                    EXCHANGE_MAP[ticker.upper()] = exchange
                    return sid
            
            logger.info(f"Discovery: no ID found in {exchange}-{ticker.lower()} page")
        except Exception as e:
            logger.warning(f"Discovery {exchange}-{ticker.lower()} failed: {e}")
    
    logger.warning(f"Discovery: could not find symbol ID for {ticker} in any exchange")
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

        # If 0 records with hardcoded ID, try rediscovering
        if len(records) == 0 and ticker.upper() in SYMBOL_IDS:
            logger.info(f"ChartExchange: 0 records with ID {symbol_id}, trying rediscovery...")
            old_id = SYMBOL_IDS.pop(ticker.upper(), None)
            new_id = _discover_symbol_id(ticker)
            if new_id and new_id != old_id:
                logger.info(f"ChartExchange: rediscovered {ticker} ID: {old_id} → {new_id}")
                payload["symbol_"] = new_id
                exchange = EXCHANGE_MAP.get(ticker.upper(), 'nyse')
                headers['Referer'] = f'https://chartexchange.com/symbol/{exchange}-{ticker.lower()}/exchange-volume/'
                resp2 = requests.post(url, json=payload, headers=headers, timeout=15)
                if resp2.status_code == 200:
                    data = resp2.json()
                    records = data.get('data', [])
                    logger.info(f"ChartExchange API retry: got {len(records)} records with new ID {new_id}")
            else:
                # Put old ID back
                if old_id:
                    SYMBOL_IDS[ticker.upper()] = old_id

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
#  CLUSTERING — Merge nearby levels into zones
# ═══════════════════════════════════════════════════════════

def _cluster_dp_levels(levels, threshold_pct=0.15):
    """
    Cluster nearby dark pool levels into zones.
    
    Levels within threshold_pct% of each other are merged.
    Result: volume-weighted average price, summed volume/trades.
    
    Example: 459.27, 459.35, 459.36, 459.40, 459.50, 459.94
    With 0.15% threshold (~$0.69 at $460):
      Cluster 1: 459.27-459.50 → one zone
      Cluster 2: 459.94 → separate zone
    """
    if not levels:
        return []
    
    # Sort by price ascending
    sorted_levels = sorted(levels, key=lambda x: x['price'])
    
    clusters = []
    current_cluster = [sorted_levels[0]]
    
    for i in range(1, len(sorted_levels)):
        lvl = sorted_levels[i]
        # Compare to the volume-weighted center of current cluster
        cluster_center = sum(l['price'] * l['volume'] for l in current_cluster) / max(sum(l['volume'] for l in current_cluster), 1)
        
        # If within threshold, add to cluster
        if abs(lvl['price'] - cluster_center) / cluster_center < (threshold_pct / 100):
            current_cluster.append(lvl)
        else:
            clusters.append(current_cluster)
            current_cluster = [lvl]
    
    clusters.append(current_cluster)  # Don't forget last cluster
    
    # Merge each cluster into a single zone
    merged = []
    for cluster in clusters:
        total_vol = sum(l['volume'] for l in cluster)
        total_trades = sum(l.get('trades', 0) for l in cluster)
        
        # Volume-weighted average price
        if total_vol > 0:
            vwap = sum(l['price'] * l['volume'] for l in cluster) / total_vol
        else:
            vwap = sum(l['price'] for l in cluster) / len(cluster)
        
        merged.append({
            'price': round(vwap, 2),
            'volume': total_vol,
            'trades': total_trades,
            'num_levels': len(cluster),  # How many raw levels merged
        })
    
    logger.info(f"Clustering: {len(levels)} raw → {len(merged)} zones "
                f"(threshold: {threshold_pct}%)")
    
    return merged


# ═══════════════════════════════════════════════════════════
#  DIRECTION — Enrich levels with Bid/Ask from Prints
# ═══════════════════════════════════════════════════════════

def enrich_levels_with_direction(levels, prints, threshold_pct=0.15):
    """
    For each DP level, check nearby prints to determine if it's Buy or Sell.
    
    Args:
        levels: list of {'strike': float, 'volume': int, 'type': str, ...}
        prints: list of {'price': float, 'size': int, 'side': str, ...}
        threshold_pct: how close a print must be to a level (% of price)
    
    Returns: levels with added 'side' field ('Buy', 'Sell', or 'Neutral')
    """
    if not prints:
        return levels
    
    for lvl in levels:
        strike = lvl['strike']
        threshold = strike * (threshold_pct / 100)
        
        # Find prints near this level
        bid_vol = 0
        ask_vol = 0
        for p in prints:
            if abs(p['price'] - strike) <= threshold:
                if 'bid' in p.get('side', '').lower():
                    bid_vol += p.get('size', 0)
                elif 'ask' in p.get('side', '').lower():
                    ask_vol += p.get('size', 0)
        
        if bid_vol > ask_vol * 1.2:
            lvl['side'] = 'Buy'
        elif ask_vol > bid_vol * 1.2:
            lvl['side'] = 'Sell'
        else:
            lvl['side'] = 'Neutral'
        
        lvl['bid_vol'] = bid_vol
        lvl['ask_vol'] = ask_vol
        logger.debug(f"Level {strike:.2f}: Bid={bid_vol:,} Ask={ask_vol:,} → {lvl['side']}")
    
    return levels


def get_buy_sell_from_levels(levels):
    """
    Extract top 2 Buy and top 2 Sell zones from enriched levels.
    Returns dict with buy1, buy2, sell1, sell2 (ETF prices).
    """
    result = {'buy1': 0.0, 'buy2': 0.0, 'sell1': 0.0, 'sell2': 0.0}
    
    buys = sorted([l for l in levels if l.get('side') == 'Buy'],
                  key=lambda x: x.get('volume', 0), reverse=True)
    sells = sorted([l for l in levels if l.get('side') == 'Sell'],
                   key=lambda x: x.get('volume', 0), reverse=True)
    
    if len(buys) >= 1:
        result['buy1'] = buys[0]['strike']
    if len(buys) >= 2:
        result['buy2'] = buys[1]['strike']
    if len(sells) >= 1:
        result['sell1'] = sells[0]['strike']
    if len(sells) >= 2:
        result['sell2'] = sells[1]['strike']
    
    # If no direction data, use top levels as fallback
    if result['buy1'] == 0 and result['sell1'] == 0 and levels:
        sorted_by_vol = sorted(levels, key=lambda x: x.get('volume', 0), reverse=True)
        if len(sorted_by_vol) >= 1:
            result['buy1'] = sorted_by_vol[0]['strike']
        if len(sorted_by_vol) >= 2:
            result['sell1'] = sorted_by_vol[1]['strike']
    
    return result


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

    # 0. Try ChartExchange Playwright (browser-based, bypasses server blocks)
    try:
        from chartexchange_dp import fetch_dp_sync
        logger.info(f"Trying ChartExchange Playwright for {ticker} DP...")
        levels_data = fetch_dp_sync(ticker)
        if levels_data and len(levels_data) >= 3:
            logger.info(f"ChartExchange Playwright SUCCESS: {len(levels_data)} levels")
    except ImportError:
        logger.info("chartexchange_dp module not available, using API...")
        levels_data = []
    except Exception as e:
        logger.warning(f"ChartExchange Playwright failed: {e}")
        levels_data = []

    # 1. Fallback: ChartExchange direct API
    if not levels_data or len(levels_data) < 3:
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

        # ── Cluster nearby levels into zones ──
        # Levels within 0.15% of each other are merged into one zone
        # Representative price = volume-weighted average, volumes summed
        clustered = _cluster_dp_levels(levels_data, threshold_pct=0.15)
        logger.info(f"Clustered {len(levels_data)} levels → {len(clustered)} zones")

        for lvl in sorted(clustered, key=lambda x: x['volume'], reverse=True)[:8]:
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
                'strike': round(strike, 2), 'type': tp,
                'volume': vol, 'trades': trades,
                'dollar_volume': strike * vol,
                'num_levels': lvl.get('num_levels', 1),
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

    # 4. Enrich levels with Buy/Sell direction from Prints
    if result['levels'] and result['source'] == 'chartexchange':
        try:
            from chartexchange_prints import fetch_prints_sync
            min_size = 5000 if ticker.upper() in ("GLD", "SLV") else 100000
            prints = fetch_prints_sync(ticker, min_size=min_size, max_prints=30)
            if prints:
                result['levels'] = enrich_levels_with_direction(result['levels'], prints)
                result['prints_count'] = len(prints)
                logger.info(f"Enriched {len(result['levels'])} levels with direction from {len(prints)} prints")
        except ImportError:
            logger.info("chartexchange_prints not available — no direction data")
        except Exception as e:
            logger.warning(f"Prints enrichment failed: {e}")

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
            side = lvl.get('side', '')
            vol_str = f"{vol:,}" if vol else "N/A"
            trade_str = f" | {trades:,} Trades" if trades else ""

            # Direction icon
            if side == 'Buy':
                side_icon = "🟢 BUY"
            elif side == 'Sell':
                side_icon = "🔴 SELL"
            else:
                side_icon = "⚪"
            
            num_levels = lvl.get('num_levels', 1)
            cluster_str = f" ({num_levels} Levels)" if num_levels > 1 else ""
            lines.append(f"  [{side_icon}] {tp}{cluster_str}:")
            lines.append(f"      {strike:.2f} {etf_label}  =  {to_cfd(strike):.2f} {cfd_label}  |  Vol: {vol_str}{trade_str}")
            lines.append("")

        lines.append(f"--- {cfd_label} INPUT ---")
        lines.append("")
        for i, lvl in enumerate(levels[:8], 1):
            side = lvl.get('side', '')
            side_tag = f" [{side}]" if side else ""
            lines.append(f"  Zone {i}: {lvl['strike']:.2f} {etf_label} = {to_cfd(lvl['strike']):.2f} {cfd_label}  ({lvl['type']}{side_tag})")
        lines.append("")

        # Buy/Sell zones for Pine Script indicator
        buy_sell = get_buy_sell_from_levels(levels)
        lines.append("--- INDIKATOR INPUT (Buy/Sell) ---")
        lines.append("")
        lines.append(f"  DP Buy 1:  {buy_sell['buy1']:.2f}" if buy_sell['buy1'] > 0 else "  DP Buy 1:  -")
        lines.append(f"  DP Buy 2:  {buy_sell['buy2']:.2f}" if buy_sell['buy2'] > 0 else "  DP Buy 2:  -")
        lines.append(f"  DP Sell 1: {buy_sell['sell1']:.2f}" if buy_sell['sell1'] > 0 else "  DP Sell 1: -")
        lines.append(f"  DP Sell 2: {buy_sell['sell2']:.2f}" if buy_sell['sell2'] > 0 else "  DP Sell 2: -")
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
