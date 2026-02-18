"""
BullNet Dark Pool Scanner
Fetches REAL dark pool prints from ChartExchange via Selenium.
Falls back to FINRA + options-derived levels if Selenium fails.
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
#  SELENIUM — ChartExchange Dark Pool Prints + Levels
# ═══════════════════════════════════════════════════════════

def _get_driver():
    """Create headless Chrome driver."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36')

    chrome_bin = os.getenv('CHROME_BIN', None)
    if chrome_bin:
        options.binary_location = chrome_bin

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver


def _get_exchange_prefix(ticker):
    """Map ticker to ChartExchange URL prefix."""
    nasdaq_tickers = {'QQQ', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'META', 'NVDA', 'TSLA', 'AMD', 'NFLX', 'AVGO', 'INTC', 'MU', 'QCOM'}
    return 'nasdaq' if ticker.upper() in nasdaq_tickers else 'nyse'


def _parse_number(txt):
    """Parse a number string like '1,234', '$1.5M', '500K'."""
    txt = txt.replace(',', '').replace('$', '').strip()
    if not txt:
        return None

    m = re.match(r'^([\d.]+)\s*([MKB])?$', txt, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        suffix = (m.group(2) or '').upper()
        if suffix == 'M':
            val *= 1_000_000
        elif suffix == 'K':
            val *= 1_000
        elif suffix == 'B':
            val *= 1_000_000_000
        return val
    return None


def fetch_chartexchange_selenium(ticker="QQQ"):
    """
    Scrape dark pool prints + levels from ChartExchange.
    Returns (prints_list, levels_list).
    """
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.warning("Selenium not installed — pip install selenium")
        return [], []

    exchange = _get_exchange_prefix(ticker)
    base = f"https://chartexchange.com/symbol/{exchange}-{ticker.lower()}/exchange-volume"
    prints_url = f"{base}/dark-pool-prints/"
    levels_url = f"{base}/dark-pool-levels/"

    driver = None
    prints_data = []
    levels_data = []

    try:
        driver = _get_driver()

        # ─── DARK POOL LEVELS (aggregated — best source) ───
        logger.info(f"Selenium: fetching DP levels → {levels_url}")
        driver.get(levels_url)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
        except:
            logger.warning("Timeout waiting for levels table")

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        logger.info(f"Selenium: found {len(rows)} level rows")

        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue
                texts = [c.text.strip() for c in cells]

                price = None
                volume = None
                trades = None

                for txt in texts:
                    val = _parse_number(txt)
                    if val is None:
                        continue

                    # Price: has decimal, reasonable range
                    if price is None and '.' in txt.replace(',', '') and 10 < val < 50000:
                        price = val
                    # Volume: large integer
                    elif volume is None and val >= 100 and '.' not in txt.replace(',', ''):
                        volume = int(val)
                    # Trades: smaller integer after volume found
                    elif trades is None and volume is not None and val < volume and val >= 1:
                        trades = int(val)

                if price and volume:
                    levels_data.append({
                        'price': price,
                        'volume': volume,
                        'trades': trades or 0,
                    })
            except:
                continue

        logger.info(f"Selenium: parsed {len(levels_data)} valid levels")

        # ─── DARK POOL PRINTS (individual trades) ───
        logger.info(f"Selenium: fetching DP prints → {prints_url}")
        driver.get(prints_url)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
        except:
            logger.warning("Timeout waiting for prints table")

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        logger.info(f"Selenium: found {len(rows)} print rows")

        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue
                texts = [c.text.strip() for c in cells]

                price = None
                shares = None
                dollar_vol = None

                for txt in texts:
                    val = _parse_number(txt)
                    if val is None:
                        continue

                    if price is None and '.' in txt.replace(',', '') and 10 < val < 50000:
                        price = val
                    elif shares is None and val >= 100 and '.' not in txt.replace(',', ''):
                        shares = int(val)
                    elif dollar_vol is None and val > 10000:
                        dollar_vol = val

                if price and shares:
                    if not dollar_vol:
                        dollar_vol = price * shares
                    prints_data.append({
                        'price': price,
                        'shares': shares,
                        'dollar_volume': dollar_vol,
                    })
            except:
                continue

        logger.info(f"Selenium: parsed {len(prints_data)} valid prints")

    except Exception as e:
        logger.error(f"Selenium error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

    return prints_data, levels_data


# ═══════════════════════════════════════════════════════════
#  FINRA Short Volume (free, no auth)
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
    1. ChartExchange Levels (Selenium) — echte aggregierte DP Daten
    2. ChartExchange Prints (Selenium) — einzelne Trades, selbst aggregiert
    3. Options-derived — Fallback aus OI/Volume
    Always includes FINRA short volume.
    """
    result = {
        'ticker': ticker,
        'timestamp': datetime.now().isoformat(),
        'source': None,
        'levels': [],
        'finra': None,
    }

    # 1. Try ChartExchange via Selenium
    try:
        prints_data, levels_data = fetch_chartexchange_selenium(ticker)
    except Exception as e:
        logger.warning(f"Selenium failed: {e}")
        prints_data, levels_data = [], []

    # Use pre-aggregated levels (best)
    if levels_data and len(levels_data) >= 3:
        result['source'] = 'chartexchange'

        for lvl in sorted(levels_data, key=lambda x: x['volume'], reverse=True)[:8]:
            strike = lvl['price']
            vol = lvl['volume']

            if spot:
                if strike > spot * 1.005:
                    tp = "DP Resistance"
                elif strike < spot * 0.995:
                    tp = "DP Support"
                else:
                    tp = "High Volume"
                if vol > 50000:
                    tp = "Block Trade"
            else:
                tp = "High Volume"

            result['levels'].append({
                'strike': strike, 'type': tp,
                'volume': vol, 'trades': lvl.get('trades', 0),
                'dollar_volume': strike * vol,
            })

        result['levels'].sort(key=lambda x: x['strike'])

    # Or aggregate prints ourselves
    elif prints_data and len(prints_data) >= 5:
        result['source'] = 'chartexchange'

        price_agg = defaultdict(lambda: {'shares': 0, 'dollar_volume': 0, 'count': 0})
        for p in prints_data:
            bucket = round(p['price'])
            price_agg[bucket]['shares'] += p['shares']
            price_agg[bucket]['dollar_volume'] += p['dollar_volume']
            price_agg[bucket]['count'] += 1

        sorted_levels = sorted(price_agg.items(), key=lambda x: x[1]['dollar_volume'], reverse=True)

        for strike, data in sorted_levels[:8]:
            if spot:
                if strike > spot * 1.005:
                    tp = "DP Resistance"
                elif strike < spot * 0.995:
                    tp = "DP Support"
                else:
                    tp = "High Volume"
                if data['dollar_volume'] > 5_000_000:
                    tp = "Block Trade"
            else:
                tp = "High Volume"

            result['levels'].append({
                'strike': float(strike), 'type': tp,
                'volume': data['shares'],
                'dollar_volume': data['dollar_volume'],
                'trades': data['count'],
            })

        result['levels'].sort(key=lambda x: x['strike'])

    # 2. Fallback: Options-derived
    if not result['levels'] and spot and gex_df is not None:
        result['source'] = 'options-derived'
        result['levels'] = derive_dp_levels_from_options(spot, gex_df)

    # 3. FINRA short volume
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
            trade_str = f" | {trades} Trades" if trades else ""

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
