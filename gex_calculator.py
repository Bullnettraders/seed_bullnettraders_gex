import requests
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import norm
import json
import logging
import re
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"
RISK_FREE_RATE = 0.045
DIVIDEND_YIELD = 0.005
MAX_EXPIRATIONS = 12
STRIKE_RANGE_PCT = 0.20


# ═══════════════════════════════════════════════════════════
#  SOURCE 1: BARCHART (Selenium) — Primary
# ═══════════════════════════════════════════════════════════

def fetch_barchart_gex(ticker="QQQ"):
    """
    Scrape Gamma Flip, Call Wall, Put Wall from Barchart.com.
    Returns dict with levels or None on failure.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.warning("Selenium not installed")
        return None

    # Barchart URL pattern
    asset_type = "etfs-funds" if ticker.upper() in ("QQQ", "SPY", "IWM", "DIA", "GLD", "SLV", "TLT", "XLF", "XLE", "VOO") else "stocks"
    url = f"https://www.barchart.com/{asset_type}/quotes/{ticker.upper()}/gamma-exposure"

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

    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(45)

        logger.info(f"Barchart: fetching GEX for {ticker} → {url}")
        driver.get(url)

        # Wait for gamma flip text to render (JS loaded content)
        import time
        max_wait = 20
        found = False
        for i in range(max_wait):
            time.sleep(1)
            page_text = driver.page_source
            if 'gamma flip' in page_text.lower() or 'gamma_flip' in page_text.lower():
                found = True
                logger.info(f"Barchart: gamma flip text found after {i+1}s")
                break
            if i % 5 == 4:
                logger.info(f"Barchart: waiting for JS... ({i+1}s)")

        if not found:
            # Try scrolling down to trigger lazy loading
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(3)
            page_text = driver.page_source
            if 'gamma flip' in page_text.lower():
                found = True
                logger.info("Barchart: gamma flip found after scroll")

        if not found:
            # Last resort: try clicking "View All Filters" or similar expand button
            try:
                buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'View All')]")
                for btn in buttons:
                    btn.click()
                    time.sleep(2)
                page_text = driver.page_source
                if 'gamma flip' in page_text.lower():
                    found = True
                    logger.info("Barchart: gamma flip found after clicking expand")
            except:
                pass

        if not found:
            logger.warning(f"Barchart: gamma flip text NOT found after {max_wait}s")
            # Debug: log a snippet of the page around key terms
            lower_text = page_text.lower()
            for term in ['gamma', 'flip', 'put wall', 'call wall', 'exposure']:
                idx = lower_text.find(term)
                if idx >= 0:
                    snippet = page_text[max(0,idx-50):idx+100].replace('\n', ' ')
                    logger.info(f"Barchart debug '{term}' at pos {idx}: ...{snippet}...")

        page_text = driver.page_source
        # Also get clean rendered text (no HTML tags)
        try:
            rendered_text = driver.find_element(By.TAG_NAME, "body").text
            logger.info(f"Barchart: rendered text length = {len(rendered_text)}")
        except:
            rendered_text = ""

        levels = {}

        # Use rendered text first (clean, no HTML tags), then page_source as fallback
        for src_name, src_text in [("rendered", rendered_text), ("html", page_text)]:
            if levels.get('gamma_flip') and levels.get('put_wall') and levels.get('call_wall'):
                break  # All found

            # Parse gamma flip
            if 'gamma_flip' not in levels:
                gf_match = re.search(r'gamma\s*flip\s*(?:point\s*)?(?:is\s*)?(\d+\.?\d*)', src_text, re.IGNORECASE)
                if not gf_match:
                    gf_match = re.search(r'gammaFlip["\s:=]+(\d+\.?\d*)', src_text, re.IGNORECASE)
                if gf_match:
                    levels['gamma_flip'] = float(gf_match.group(1))
                    logger.info(f"Barchart [{src_name}]: Gamma Flip = {levels['gamma_flip']}")

            # Parse put wall
            if 'put_wall' not in levels:
                pw_match = re.search(r'put\s*wall\s*(?:is\s*)?(\d+\.?\d*)', src_text, re.IGNORECASE)
                if not pw_match:
                    pw_match = re.search(r'putWall["\s:=]+(\d+\.?\d*)', src_text, re.IGNORECASE)
                if pw_match:
                    levels['put_wall'] = float(pw_match.group(1))
                    logger.info(f"Barchart [{src_name}]: Put Wall = {levels['put_wall']}")

            # Parse call wall
            if 'call_wall' not in levels:
                cw_match = re.search(r'call\s*wall\s*(?:is\s*)?(\d+\.?\d*)', src_text, re.IGNORECASE)
                if not cw_match:
                    cw_match = re.search(r'callWall["\s:=]+(\d+\.?\d*)', src_text, re.IGNORECASE)
                if cw_match:
                    levels['call_wall'] = float(cw_match.group(1))
                    logger.info(f"Barchart [{src_name}]: Call Wall = {levels['call_wall']}")

        # Debug: if walls still missing, log what's around them
        if 'put_wall' not in levels:
            for src_name, src_text in [("rendered", rendered_text), ("html", page_text)]:
                idx = src_text.lower().find('put wall')
                if idx >= 0:
                    snippet = src_text[idx:idx+150].replace('\n', ' ')
                    logger.info(f"Barchart debug [{src_name}] put_wall: {snippet}")
        if 'call_wall' not in levels:
            for src_name, src_text in [("rendered", rendered_text), ("html", page_text)]:
                idx = src_text.lower().find('call wall')
                if idx >= 0:
                    snippet = src_text[idx:idx+150].replace('\n', ' ')
                    logger.info(f"Barchart debug [{src_name}] call_wall: {snippet}")

        # Get spot price from page
        if 'spot' not in levels:
            for src_name, src_text in [("rendered", rendered_text), ("html", page_text)]:
                spot_match = re.search(r'Last\s*Price[:\s]*(\d+\.?\d*)', src_text, re.IGNORECASE)
                if not spot_match:
                    spot_match = re.search(r'(\d{2,4}\.\d{2})\s*[-+]?\d+\.\d+\s*\(', src_text)
                if spot_match:
                    levels['spot'] = float(spot_match.group(1))
                    logger.info(f"Barchart [{src_name}]: Spot = {levels['spot']}")
                    break

        # Derive regime
        if 'gamma_flip' in levels and 'spot' in levels:
            levels['gamma_regime'] = "Positiv" if levels['spot'] > levels['gamma_flip'] else "Negativ"
        elif 'gamma_flip' in levels:
            levels['gamma_regime'] = "N/A"

        if 'gamma_flip' in levels:
            levels['source'] = 'barchart'
            logger.info(f"Barchart SUCCESS: GF={levels.get('gamma_flip')} CW={levels.get('call_wall')} PW={levels.get('put_wall')}")
            return levels
        else:
            logger.warning("Barchart: could not parse gamma flip from page")
            return None

    except Exception as e:
        logger.error(f"Barchart error: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


# ═══════════════════════════════════════════════════════════
#  SOURCE 2: CBOE API — Fallback
# ═══════════════════════════════════════════════════════════

def bs_gamma(S, K, T, r, q, sigma):
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return np.exp(-q * T) * norm.pdf(d1) / (S * sigma * np.sqrt(T))


def parse_option_symbol(symbol):
    match = re.search(r'(\d{6})([CP])(\d{8})$', symbol)
    if not match:
        return None
    date_str = match.group(1)
    opt_type = 'call' if match.group(2) == 'C' else 'put'
    strike = int(match.group(3)) / 1000.0
    try:
        exp_date = datetime.strptime(date_str, '%y%m%d')
    except:
        return None
    return exp_date, opt_type, strike


def fetch_cboe_options(ticker="QQQ"):
    url = CBOE_URL.format(ticker=ticker)
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    spot = data.get('data', {}).get('current_price', None)
    if spot is None:
        spot = data.get('data', {}).get('close', None)
    if spot is None:
        for key in ['last_trade_price', 'prev_day_close', 'bid', 'ask']:
            val = data.get('data', {}).get(key)
            if val and val > 0:
                spot = val
                break
    options = data.get('data', {}).get('options', [])
    logger.info(f"CBOE: Spot ${spot:.2f} | Contracts: {len(options)}")
    return spot, options


def parse_options(spot, options):
    records = []
    now = datetime.now()
    skipped = {'no_symbol': 0, 'no_strike': 0, 'out_of_range': 0, 'expired': 0, 'no_iv': 0}

    for opt in options:
        try:
            symbol = opt.get('option', '')
            if not symbol:
                skipped['no_symbol'] += 1
                continue
            parsed = parse_option_symbol(symbol)
            if parsed is None:
                skipped['no_symbol'] += 1
                continue
            exp_date, opt_type, strike = parsed
            if strike <= 0:
                skipped['no_strike'] += 1
                continue
            if abs(strike - spot) / spot > STRIKE_RANGE_PCT:
                skipped['out_of_range'] += 1
                continue
            dte = (exp_date - now).days
            if dte < 0:
                skipped['expired'] += 1
                continue
            T = max(dte / 365.0, 1 / 365.0)
            oi = opt.get('open_interest', 0) or 0
            volume = opt.get('volume', 0) or 0
            iv = opt.get('iv', 0) or 0
            gamma_raw = opt.get('gamma', 0) or 0
            bid = opt.get('bid', 0) or 0
            ask = opt.get('ask', 0) or 0
            if iv <= 0:
                if bid <= 0 and ask <= 0:
                    skipped['no_iv'] += 1
                    continue
                iv = 0.20
            records.append({
                'strike': strike, 'type': opt_type, 'expiration': exp_date,
                'dte': dte, 'T': T, 'oi': int(oi), 'volume': int(volume),
                'iv': iv, 'gamma': gamma_raw, 'bid': bid, 'ask': ask,
            })
        except:
            continue

    df = pd.DataFrame(records)
    logger.info(f"CBOE: Parsed {len(df)} contracts | Skipped: {skipped}")
    return df


def calculate_gex(spot, df):
    if df.empty:
        return pd.DataFrame()
    exp_dates = sorted(df['expiration'].unique())[:MAX_EXPIRATIONS]
    df = df[df['expiration'].isin(exp_dates)].copy()
    gammas = []
    for _, row in df.iterrows():
        if row['gamma'] > 0:
            gammas.append(row['gamma'])
        else:
            gammas.append(bs_gamma(spot, row['strike'], row['T'], RISK_FREE_RATE, DIVIDEND_YIELD, row['iv']))
    df['calc_gamma'] = gammas
    df['gex'] = df['calc_gamma'] * df['oi'] * 100 * spot * spot * 0.01
    df['dealer_gex'] = df.apply(lambda r: r['gex'] if r['type'] == 'call' else -r['gex'], axis=1)
    gex_by_strike = df.groupby('strike').agg(
        call_gex=('dealer_gex', lambda x: x[df.loc[x.index, 'type'] == 'call'].sum()),
        put_gex=('dealer_gex', lambda x: x[df.loc[x.index, 'type'] == 'put'].sum()),
        net_gex=('dealer_gex', 'sum'),
        total_oi=('oi', 'sum'),
        total_volume=('volume', 'sum'),
    ).reset_index().sort_values('strike')
    return gex_by_strike


def find_key_levels(spot, gex_df):
    levels = {}
    if gex_df.empty:
        return levels
    call_data = gex_df[gex_df['call_gex'] > 0]
    if not call_data.empty:
        idx = call_data['call_gex'].idxmax()
        levels['call_wall'] = call_data.loc[idx, 'strike']
    put_data = gex_df[gex_df['put_gex'] < 0]
    if not put_data.empty:
        idx = put_data['put_gex'].idxmin()
        levels['put_wall'] = put_data.loc[idx, 'strike']
    sorted_gex = gex_df.sort_values('strike')
    net_vals = sorted_gex['net_gex'].values
    strikes = sorted_gex['strike'].values
    gamma_flip = None
    min_dist = float('inf')
    for i in range(len(net_vals) - 1):
        if net_vals[i] * net_vals[i + 1] < 0:
            ratio = abs(net_vals[i]) / (abs(net_vals[i]) + abs(net_vals[i + 1]))
            fp = strikes[i] + ratio * (strikes[i + 1] - strikes[i])
            dist = abs(fp - spot)
            if dist < min_dist:
                min_dist = dist
                gamma_flip = fp
    if gamma_flip is not None:
        levels['gamma_flip'] = round(gamma_flip, 2)
        levels['gamma_regime'] = "Positiv" if spot > gamma_flip else "Negativ"
    if gex_df['total_volume'].sum() > 0:
        idx = gex_df['total_volume'].idxmax()
        levels['hvl'] = gex_df.loc[idx, 'strike']
    gex_df['abs_gex'] = gex_df['call_gex'].abs() + gex_df['put_gex'].abs()
    idx = gex_df['abs_gex'].idxmax()
    levels['abs_gamma_strike'] = gex_df.loc[idx, 'strike']
    return levels


# ═══════════════════════════════════════════════════════════
#  DISCORD FORMAT
# ═══════════════════════════════════════════════════════════

def format_discord_message(spot, levels, ratio=41.33, ticker="QQQ"):
    def to_cfd(p):
        return round(p * ratio, 2)
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    gf = levels.get('gamma_flip', 0)
    cw = levels.get('call_wall', 0)
    pw = levels.get('put_wall', 0)
    hvl = levels.get('hvl', 0)
    regime = levels.get('gamma_regime', 'N/A')
    source = levels.get('source', 'cboe')

    is_gold = ticker.upper() in ("GLD", "GOLD")
    etf_label = "GLD" if is_gold else "QQQ"
    cfd_label = "XAUUSD" if is_gold else "NAS100 CFD"
    title = f"BullNet GEX Report - {'GOLD' if is_gold else ticker.upper()}"

    hint = "UEBER Flip = Range/Magnet" if regime == "Positiv" else "UNTER Flip = Acceleration"

    lines = []
    lines.append(title)
    lines.append("=" * 44)
    lines.append(f"  {now}  |  {etf_label}: ${spot:.2f}  |  Source: {source}")
    lines.append("")
    lines.append(f"  Gamma Regime: {regime.upper()}")
    lines.append(f"  {hint}")
    lines.append("")
    lines.append("--- KEY LEVELS ---")
    lines.append(f"  Gamma Flip:   {gf:.2f} {etf_label}  =  {to_cfd(gf):.2f} {cfd_label}")
    lines.append(f"  Call Wall:    {cw:.2f} {etf_label}  =  {to_cfd(cw):.2f} {cfd_label}")
    lines.append(f"  Put Wall:     {pw:.2f} {etf_label}  =  {to_cfd(pw):.2f} {cfd_label}")
    if hvl:
        lines.append(f"  HVL:          {hvl:.2f} {etf_label}  =  {to_cfd(hvl):.2f} {cfd_label}")
    lines.append("")
    lines.append(f"--- {cfd_label} INPUT ---")
    lines.append(f"  Gamma Flip:    {to_cfd(gf):.2f}")
    lines.append(f"  Call Wall:     {to_cfd(cw):.2f}")
    lines.append(f"  Put Wall:      {to_cfd(pw):.2f}")
    if hvl:
        lines.append(f"  HVL:           {to_cfd(hvl):.2f}")
    lines.append("")
    lines.append("=" * 44)
    lines.append(f"  Ratio: {ratio:.4f} | {source.upper()} (15min delayed)")
    return "```\n" + "\n".join(lines) + "\n```"


# ═══════════════════════════════════════════════════════════
#  MAIN — Barchart first, CBOE fallback
# ═══════════════════════════════════════════════════════════

def run(ticker="QQQ", ratio=41.33):
    """
    Get GEX levels. Priority:
    1. Barchart (Selenium) — professional data, accurate Gamma Flip
    2. CBOE API (own calc) — fallback if Selenium fails
    """
    spot = None
    levels = None
    gex_df = None

    # 1. Try Barchart first
    try:
        logger.info(f"Trying Barchart for {ticker}...")
        bc_levels = fetch_barchart_gex(ticker)
        if bc_levels and 'gamma_flip' in bc_levels:
            spot = bc_levels.get('spot', 0)
            levels = bc_levels

            # Still fetch CBOE for HVL and gex_df (needed for dark pool fallback)
            try:
                cboe_spot, options = fetch_cboe_options(ticker)
                if not spot or spot == 0:
                    spot = cboe_spot
                df = parse_options(cboe_spot, options)
                if not df.empty:
                    gex_df = calculate_gex(cboe_spot, df)
                    cboe_levels = find_key_levels(cboe_spot, gex_df)
                    # Add HVL from CBOE if Barchart doesn't have it
                    if 'hvl' not in levels and 'hvl' in cboe_levels:
                        levels['hvl'] = cboe_levels['hvl']
            except Exception as e:
                logger.warning(f"CBOE supplement failed: {e}")

            logger.info(f"Using Barchart data: GF={levels.get('gamma_flip')} CW={levels.get('call_wall')} PW={levels.get('put_wall')}")
            return spot, levels, gex_df
    except Exception as e:
        logger.warning(f"Barchart failed: {e}")

    # 2. Fallback to CBOE
    try:
        logger.info(f"Falling back to CBOE for {ticker}...")
        spot, options = fetch_cboe_options(ticker)
        if not options:
            return None, None, None
        df = parse_options(spot, options)
        if df.empty:
            return None, None, None
        gex_df = calculate_gex(spot, df)
        levels = find_key_levels(spot, gex_df)
        levels['source'] = 'cboe'
        return spot, levels, gex_df
    except Exception as e:
        logger.error(f"CBOE also failed: {e}")
        raise


if __name__ == "__main__":
    spot, levels, _ = run()
    if levels:
        print(format_discord_message(spot, levels))
